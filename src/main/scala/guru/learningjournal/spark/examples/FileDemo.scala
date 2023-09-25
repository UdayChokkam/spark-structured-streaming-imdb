package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object FileDemo extends Serializable {

  case class TitleRatings(tconst: String, averageRating: Double, numVotes: Int)
  case class TitleAkas(titleId: String, ordering: Int, title: String,
                       region: String, language: String, types: String, attributes: String, isOriginalTitle: Int)
  case class TitlePrincipals(tconst: String, ordering: Int, nconst: String, category: String, job:String, characters: String)
  case class NameBasics(nconst: String, primaryName: String, birthYear: Int, deathYear: String, primaryProfession: String, knownForTitles: String)
  case class Top10(tconst: String, averageRating: Double, numVotes: Int, ranking: Double)
  case class groupedTitles(titleId: String, titles: Seq[String])

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

    import spark.implicits._
    val titleRatingsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/title.ratings.tsv")
      .as[TitleRatings]

    val titlesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/title.akas.tsv")
      .as[TitleAkas]

    val creditsDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/title.principals.tsv")
      .as[TitlePrincipals]

    val namesDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/name.basics.tsv")
      .as[NameBasics]

    val topDF = calculateTop10(titleRatingsDF)
    val allTitlesDF = collectAllTitles(titlesDF)
    val credited = populateCredits(creditsDF, namesDF)
    val joined1DF = topDF.join(allTitlesDF, col(tconst) === col(titleId), inner).drop(titleId)
    val joined2DF = joined1DF.join(credited, Seq(tconst), inner)

    topDF.show()
    allTitlesDF.show()
    credited.show()
    joined1DF.show()
    joined2DF.show()
  }

  def calculateTop10(titleRatingsDF: Dataset[TitleRatings]): DataFrame = {
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

  def collectAllTitles(titlesDF: Dataset[TitleAkas]): DataFrame = {
    val groupedTitlesDF = titlesDF.groupBy(titleId).agg(collect_list(title).as(titles))
    groupedTitlesDF
  }

  def populateCredits(creditsDF: Dataset[TitlePrincipals], namesDF: Dataset[NameBasics]): DataFrame = {
    val creditedNamesDF = creditsDF.join(namesDF, Seq(nconst), inner)
    val groupedCreditsDF = creditedNamesDF.groupBy(tconst).agg(collect_list(primaryName).as(credited))
    groupedCreditsDF
  }
}