package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, current_timestamp, desc, lit, round, sum, window}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object FileStream1Demo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Imdb Streaming")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    import spark.implicits._
    val rawDF = spark.readStream
      .format("csv")
      .option("delimiter", "\t")
      .option("path", "inputstream")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load()
      .withColumn("sno", lit(1))

    val raw1DF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("path", "inputstream/title.ratings.tsv")
      .option("header", "true")
      .load()
      .withColumn("sno", lit(1))

/*    val prDF = rawDF.withColumn("timestamp", current_timestamp())
      .groupBy(window(col("timestamp"), "1 second"))
      .agg(avg("numVotes").alias("average"))*/
    val avgDF = raw1DF.groupBy("sno").agg(avg("numVotes").as("average"))

    val rDF = rawDF.join(avgDF, Seq("sno"), "inner")
   .withColumn(
      "ranking",
      round((col("numVotes").cast(IntegerType) / col("average")) *
        col("averageRating").cast(DoubleType), 2))


    val wordCountQuery = rDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-stream-dir")
      .start()
    wordCountQuery.awaitTermination()
  }
}