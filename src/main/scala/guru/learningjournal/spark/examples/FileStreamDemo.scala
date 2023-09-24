package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDateTime


object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // Define a case class to represent salary data
  case class TitleRating(tconst: String, averageRating: Double, numVotes: Long)
  case class TitleRatingWithAverage(tconst: String, averageRating: Double, numVotes: Long, average:Double)

  case class TitleRatingState(sum: Long, count: Long)

  def updateStateWithAverage(tconst: String, records: Iterator[TitleRating], state: GroupState[TitleRatingState]
                            ): Iterator[Seq[TitleRatingWithAverage]] = {
    val newState = state.getOption.getOrElse(TitleRatingState(0, 0))
    val recordsSeq = records.toSeq
    val newSum = recordsSeq.map(_.numVotes).sum + newState.sum
    val newCount = recordsSeq.size + newState.count
    val average:Double = newSum/ newCount
    val TitleRatingWithAverages:Seq[TitleRatingWithAverage] = recordsSeq.map(x => TitleRatingWithAverage(x.tconst, x.averageRating, x.numVotes,average))

    logger.info("new sum")
    logger.info(newSum)

    state.update(TitleRatingState(newSum, newCount))
    if (newCount != 0) {
      Iterator(TitleRatingWithAverages)
    } else {
      Iterator(TitleRatingWithAverages)
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Imdb Streaming")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
    import spark.implicits._

    /*    val titleRatingsDF = spark.read
          .format("csv")
          .option("delimiter", "\t")
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)
          .load("input/title.ratings.tsv")*/

    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
      .as[String]


    // Define a schema for the input data
    val schema = "tconst STRING, averageRating DOUBLE, numVotes LONG"

    // Parse the CSV data by specifying the schema
    val titleRatingsDF = socketDF
      .map(_.split("\t"))
      .map(attributes => TitleRating(attributes(0), attributes(1).toDouble, attributes(2).toLong))

   /* val parsedDF = socketDF
      .selectExpr(s"cast(value as $schema)")*/

/*
import spark.implicits._
    val statefulAvgDF = titleRatingsDF
      .groupByKey(record => record.tconst)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout()
      )(updateStateWithAverage)

   // val topDF = calculateTop10(titleRatingsDF)
    val wordCountQuery = statefulAvgDF.writeStream
      .format("console")
      //.option("numRows", 2)
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    wordCountQuery.awaitTermination()*/


    /* val titlesDF = spark.read
       .format("csv")
       .option("delimiter", "\t")
       .option("header", "true")
       .option("maxFilesPerTrigger", 1)
       .load("input/title.akas.tsv")

     val creditsDF = spark.read
       .format("csv")
       .option("delimiter", "\t")
       .option("header", "true")
       .option("maxFilesPerTrigger", 1)
       .load("input/title.principals.tsv")*/

    /* val namesDF = spark.read
       .format("csv")
       .option("delimiter", "\t")
       .option("header", "true")
       .option("maxFilesPerTrigger", 1)
       .load("input/name.basics.tsv")*/

    val topDF = calculateTop10(titleRatingsDF)
    // val topDF = calculateTop10(titleRatingsDF)
    val wordCountQuery = topDF.writeStream
      .format("console")
      //.option("numRows", 2)
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    wordCountQuery.awaitTermination()
    //val allTitlesDF = collectAllTitles(titlesDF)
    //val credited = populateCredits(creditsDF, namesDF)
    // topDF.show()
    // allTitlesDF.show()
    //credited.show()


    /*
        val joined1DF = topDF.join(allTitlesDF, col("tconst") === col("titleId"), "inner").drop("titleId")
        logger.info("Grouped Titles:")
        joined1DF.show()

        val joined2DF = joined1DF.join(credited, Seq("tconst"), "inner")
        logger.info("Final:")
        joined2DF.show()
    */

    // why both columns shown in the dataframe after joining when column names are not same
    // printschema in streaming is a challenge why and work around
    //when will the query get terminate naturally

  /*  val finalQuery = topDF.writeStream
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

  def calculateTop10(titleRatingsDF: Dataset[TitleRating]): DataFrame = {
    //logger.info("titleRatingsDF:")
    // titleRatingsDF.show()
    val filteredTitleRatingsDF = titleRatingsDF.filter(col("numVotes") >= 500).withColumn("timestamp", current_timestamp())

    //logger.info("filteredTitleRatingsDF:")
    // filteredTitleRatingsDF.show()
    //val avgDF = filteredTitleRatingsDF.groupBy("tconst").agg(avg("numVotes").as("average"))
    //val windowSpec = Window.orderBy()
    //val allRowsAvgDF = filteredTitleRatingsDF.withColumn("average", avg("numVotes").over(windowSpec))
    //val avgValue = filteredTitleRatingsDF.select(avg("numVotes")).first().getDouble(0)
    val windowSpec = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val rollingAvgDF = filteredTitleRatingsDF
      .withColumn("average", avg("numVotes").over(windowSpec))
    val rankedDF = rollingAvgDF
      .orderBy(desc("average"))
      .withColumn("rank", dense_rank().over(Window.orderBy(desc("average"))))

    // logger.info("Average value:")
    //logger.info(avgValue)
    // 5.7 1992 9503/5(1900.6)
    /* val calculatedDF = filteredTitleRatingsDF.withColumn(
       "ranking",
       (col("numVotes").cast(IntegerType) / avgValue) *
         col("averageRating").cast(DoubleType)
     )*/
/*    val rankedDF = filteredTitleRatingsDF
      .groupBy().agg(avg("numVotes").alias("average"))
      // .join(avgDF, Seq("tconst"), "inner")
      .withColumn("Ranking", col("numVotes") / col("average") * col("averageRating"))
      //.select("tconst", "Ranking")
      .orderBy(desc("Ranking"))
      .limit(2)*/
    //  logger.info("calculated:")
    //calculatedDF.show()
    //val orderedDF = calculatedDF.orderBy(col("ranking").desc)
    //logger.info("Ordered:")
    //orderedDF.show()
    //val topDF = orderedDF.limit(2)
    // logger.info("Top:")
    //topDF.show()
    rankedDF
  }

  def collectAllTitles(titlesDF: DataFrame): DataFrame = {

    // logger.info("Titles:")
    //titlesDF.show()
    // first grouping by is better
    val groupedTitlesDF = titlesDF.groupBy("titleId").agg(collect_list("title").as("titles"))
    //  logger.info("Grouped Titles:")
    //groupedTitlesDF.show()
    groupedTitlesDF
  }

  def populateCredits(creditsDF: DataFrame, namesDF: DataFrame): DataFrame = {

    // logger.info("Credits:")
    //creditsDF.show()

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
