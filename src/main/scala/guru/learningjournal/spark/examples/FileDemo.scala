package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object FileDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Imdb Streaming")
    .getOrCreate()
  private val tconst = "tconst"
  private val title = "title"
  private val titleId = title + "Id"
  private val inner = "inner"
  private val numVotes = "numVotes"
  private val ranking = "ranking"
  private val averageRating = "averageRating"
  private val nconst = "nconst"
  private val primaryName = "primaryName"
  private val credited = "Credited"
  private val titles = "titles"

  def main(args: Array[String]): Unit = {

    val topDF = calculateTop10()
    val allTitlesDF = collectAllTitles()
    val credited = populateCredits()
    val joined1DF = topDF.join(allTitlesDF, col(tconst) === col(titleId), inner).drop(titleId)
    val joined2DF = joined1DF.join(credited, Seq(tconst), inner)

    topDF.show()
    allTitlesDF.show()
    credited.show()
    joined1DF.show()
    joined2DF.show()
  }

  private def calculateTop10(): DataFrame = {

    val titleRatingsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.ratings.tsv")

    val filteredTitleRatingsDF = titleRatingsDF.filter(col(numVotes) >= 500)
    val avgValue = filteredTitleRatingsDF.select(avg(numVotes)).first().getDouble(0)
    val calculatedDF = filteredTitleRatingsDF.withColumn(
      ranking,
      (col(numVotes).cast(IntegerType) / avgValue) *
        col(averageRating).cast(DoubleType)
    )
    val orderedDF = calculatedDF.orderBy(col(ranking).desc)
    val topDF = orderedDF.limit(2)
    topDF
  }

  private def collectAllTitles(): DataFrame = {

    val titlesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.akas.tsv")
    val groupedTitlesDF = titlesDF.groupBy(titleId).agg(collect_list(title).as(titles))
    groupedTitlesDF
  }

  private def populateCredits(): DataFrame = {
    val creditsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/title.principals.tsv")

    val namesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load("input/name.basics.tsv")

    val creditedNamesDF = creditsDF.join(namesDF, Seq(nconst), inner)
    val groupedCreditsDF = creditedNamesDF.groupBy(tconst).agg(collect_list(primaryName).as(credited))
    groupedCreditsDF
  }
}