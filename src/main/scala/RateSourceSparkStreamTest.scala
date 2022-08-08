import com.codahale.metrics.{Gauge, Metric, MetricRegistry, MetricSet}
import org.apache.spark.metrics.source.StatsSource
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.slf4j.LoggerFactory

import java.util

object RateSourceSparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("RateSourceStreamTest")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val stats = new PartitionStats(spark.sparkContext)
    val statsMap = stats.getStatsMap

    val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(stats)

    SparkEnv.get.metricsSystem.registerSource(
      StatsSource("RateSourceSparkStreamTest", metricRegistry)
    )

    import spark.implicits._

    spark
      .readStream
      .format("rate")
      .option("numPartitions", 120)
      .option("rowsPerSecond", 12000)
      .load()
      .map(row => row.mkString("##"))
      .writeStream
      .foreach(new ForeachWriterImpl(statsMap))
      .start()

    spark.streams.awaitAnyTermination()

    spark.close
  }
}

class ForeachWriterImpl(statsMap: Map[PartitionStats.Value, LongAccumulator]) extends ForeachWriter[String] {
  private final val LOGGER = LoggerFactory.getLogger(this.getClass)

  override def open(partitionId: Long, epochId: Long): Boolean = {
    LOGGER.info(s"Open partition $partitionId, epoch $epochId")
    PartitionStats.incMetric(statsMap, PartitionStats.partitionsOpened, 1)
    true
  }

  override def process(value: String): Unit = {
    LOGGER.info(s"Process value: $value")
    PartitionStats.incMetric(statsMap, PartitionStats.partitionsProcessed, 1)
  }

  override def close(errorOrNull: Throwable): Unit = {
    LOGGER.info(s"Close partition: $errorOrNull")
    PartitionStats.incMetric(statsMap, PartitionStats.partitionsClosed, 1)
  }
}

object PartitionStats extends Enumeration {
  private final val LOGGER = LoggerFactory.getLogger(this.getClass)

  final val partitionsOpened = Value("partitionsOpened")
  final val partitionsProcessed = Value("partitionsProcessed")
  final val partitionsClosed = Value("partitionsClosed")

  def incMetric(statsMap: Map[PartitionStats.Value, LongAccumulator], stat: PartitionStats.Value, count: Long): Unit = {
    statsMap.get(stat) match {
      case Some(acc) => acc.add(count)
      case _ => LOGGER.error(s"Cannot increment accumulator for $stat")
    }
  }
}

class PartitionStats(sparkContext: SparkContext) extends MetricSet {
  private final val statsMap: Map[PartitionStats.Value, LongAccumulator] =
    PartitionStats.values.unsorted.map(elem => elem -> sparkContext.longAccumulator(elem.toString)).toMap

  def getStatsMap: Map[PartitionStats.Value, LongAccumulator] = statsMap

  override def getMetrics: util.Map[String, Metric] = {
    val metricsMap: Map[String, Metric] = statsMap.map(
      e =>
        (
          e._1.toString,
          new Gauge[Long]() {
            override def getValue: Long = {
              val metricValue = e._2.value

              e._2.reset() // this is possibly the problem!!!!

              metricValue
            }
          }
        )
    )

    import scala.jdk.CollectionConverters._
    metricsMap.asJava
  }
}

package org.apache.spark.metrics.source {
  case class StatsSource(srcName: String, registry: MetricRegistry) extends Source {
    override def sourceName: String = srcName

    override def metricRegistry: MetricRegistry = registry
  }
}