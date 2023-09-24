package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType}


object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Imdb Streaming")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val topDF = calculateTop10()
    val allTitlesDF = collectAllTitles()
    val credited = populateCredits()
    topDF.show()
    allTitlesDF.show()
    credited.show()

    val joined1DF = topDF.join(allTitlesDF, col("tconst") === col("titleId"), "inner").drop("titleId")
    logger.info("Grouped Titles:")
    joined1DF.show()

    val joined2DF = joined1DF.join(credited, Seq("tconst"), "inner")
    logger.info("Final:")
    joined2DF.show()

    // why both columns shown in the dataframe after joining when column names are not same
    // printschema in streaming is a challenge why and work around
    //when will the query get terminate naturally

    /* val finalQuery = rankedDF.writeStream
       .format("csv")
       .queryName("comma separated Imdb")
       .outputMode("append")
       .option("path", "output")
       .option("checkpointLocation", "chk-point-dir")
       .trigger(Trigger.ProcessingTime("1 minute"))
       .start()

     logger.info("Comma separated Imdb started")
     finalQuery.awaitTermination()*/
  }

  private def calculateTop10(): DataFrame = {

    val titleRatingsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.ratings.tsv")
    //logger.info("titleRatingsDF:")
   // titleRatingsDF.show()
    val filteredTitleRatingsDF = titleRatingsDF.filter(col("numVotes") >= 500)
    //logger.info("filteredTitleRatingsDF:")
   // filteredTitleRatingsDF.show()
    val avgValue = filteredTitleRatingsDF.select(avg("numVotes")).first().getDouble(0)
   // logger.info("Average value:")
    //logger.info(avgValue)
    // 5.7 1992 9503/5(1900.6)
    val calculatedDF = filteredTitleRatingsDF.withColumn(
      "ranking",
      (col("numVotes").cast(IntegerType) / avgValue) *
        col("averageRating").cast(DoubleType)
    )
  //  logger.info("calculated:")
    //calculatedDF.show()
    val orderedDF = calculatedDF.orderBy(col("ranking").desc)
    //logger.info("Ordered:")
    //orderedDF.show()
    val topDF = orderedDF.limit(2)
   // logger.info("Top:")
    //topDF.show()
    topDF
  }

  private def collectAllTitles(): DataFrame = {

    val titlesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.akas.tsv")
   // logger.info("Titles:")
    //titlesDF.show()
    // first grouping by is better
    val groupedTitlesDF = titlesDF.groupBy("titleId").agg(collect_list("title").as("titles"))
  //  logger.info("Grouped Titles:")
    //groupedTitlesDF.show()
    groupedTitlesDF
  }

  private def populateCredits(): DataFrame = {
    val creditsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.principals.tsv")

   // logger.info("Credits:")
    //creditsDF.show()

    val namesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/name.basics.tsv")

   // logger.info("Names:")
    //namesDF.show()

    val joined2DF = creditsDF.join(namesDF, Seq("nconst"), "inner")
   // logger.info("Joined credits and names:")
    //joined2DF.show()
    // first grouping by is better
    val groupedCreditsDF = joined2DF.groupBy("tconst").agg(collect_list("primaryName").as("Credited"))
    //logger.info("Joined credits and names grouped:")

    //logger.info("Often credited:")
    //groupedCreditsDF.show()
    groupedCreditsDF
  }
}
