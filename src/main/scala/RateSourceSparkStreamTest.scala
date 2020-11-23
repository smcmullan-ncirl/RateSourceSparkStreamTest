import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class RateSourceSparkStreamTest {
}

object RateSourceSparkStreamTest {
  private val LOGGER = LoggerFactory.getLogger(classOf[RateSourceSparkStreamTest])

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

    streamDF
      .writeStream
      .format("console")
      .start

    spark.streams.awaitAnyTermination()

    spark.close
  }
}