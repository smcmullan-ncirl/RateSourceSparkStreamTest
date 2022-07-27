import org.apache.spark.metrics.source.LongAccumulatorSource
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object RateSourceSparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("RateSourceStreamTest")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val stats = new PartitionStats(spark.sparkContext)
    LongAccumulatorSource.register(spark.sparkContext, stats.statsMap)

    import spark.implicits._

    spark
      .readStream
      .format("rate")
      .option("numPartitions", 12)
      .option("rowsPerSecond", 120)
      .load()
      .map(row => row.mkString("##"))
      .writeStream
      .foreach(new ForeachWriterImpl(stats))
      .start()

    spark.streams.awaitAnyTermination()

    spark.close
  }
}

class ForeachWriterImpl(stats: PartitionStats) extends ForeachWriter[String] {
  private final val LOGGER = LoggerFactory.getLogger(this.getClass)

  override def open(partitionId: Long, epochId: Long): Boolean = {
    LOGGER.info(s"Open partition $partitionId, epoch $epochId")
    stats.incMetric(PartitionStats.partitionsOpened, 1)
    true
  }

  override def process(value: String): Unit = {
    LOGGER.info(s"Process value: $value")
    stats.incMetric(PartitionStats.partitionsProcessed, 1)
  }

  override def close(errorOrNull: Throwable): Unit = {
    LOGGER.info(s"Close partition: $errorOrNull")
    stats.incMetric(PartitionStats.partitionsClosed, 1)
  }
}

object PartitionStats extends Enumeration {
  final val partitionsOpened = Value("partitionsOpened")
  final val partitionsProcessed = Value("partitionsProcessed")
  final val partitionsClosed = Value("partitionsClosed")
}

class PartitionStats(@transient sparkContext: SparkContext) extends Serializable {
  private final val LOGGER = LoggerFactory.getLogger(this.getClass)

  val statsMap: Map[String, LongAccumulator] =
    PartitionStats.values.unsorted.map(elem => elem.toString -> sparkContext.longAccumulator(elem.toString)).toMap

  def incMetric(stat: PartitionStats.Value, count: Long): Unit = {
    statsMap.get(stat.toString) match {
      case Some(acc) => acc.add(count)
      case _ => LOGGER.error(s"Cannot increment accumulator for $stat")
    }
  }
}