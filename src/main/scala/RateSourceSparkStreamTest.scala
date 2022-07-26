import org.apache.spark.metrics.source.LongAccumulatorSource
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object RateSourceSparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("RateSourceStreamTest")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val streamDF = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load

    streamDF.printSchema

    val stats = new PartitionStats(spark.sparkContext)

    LongAccumulatorSource.register(spark.sparkContext, stats.statsMap)

    streamDF
      .writeStream
      .foreach(new ForeachWriterImpl(stats))
      .format("console")
      .start

    spark.streams.awaitAnyTermination()

    spark.close
  }
}

class ForeachWriterImpl(stats: PartitionStats) extends ForeachWriter[Row] {
  override def open(partitionId: Long, epochId: Long): Boolean = {
    stats.incMetric(PartitionStats.partitionsOpened, 1)
    true
  }

  override def process(value: Row): Unit = {
    stats.incMetric(PartitionStats.partitionsProcessed, 1)
  }

  override def close(errorOrNull: Throwable): Unit = {
    stats.incMetric(PartitionStats.partitionsClosed, 1)
  }
}

object PartitionStats extends Enumeration {
  type PartitionStats = Value

  final val partitionsOpened = Value("partitionsOpened")
  final val partitionsProcessed = Value("partitionsProcessed")
  final val partitionsClosed = Value("partitionsClosed")
}

class PartitionStats(sparkContext: SparkContext) {
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