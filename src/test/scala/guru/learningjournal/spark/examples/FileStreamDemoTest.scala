package guru.learningjournal.spark.examples

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import guru.learningjournal.spark.examples.FileStreamDemo.{calculateTop10, collectAllTitles, populateCredits}

class RowDemoTest extends FunSuite with BeforeAndAfterAll{

  // why var  is a problem
  @transient val spark: SparkSession = SparkSession.builder()
    .appName("Demo Row Test")
    .master("local[3]")
    .getOrCreate()
  @transient var myDF: DataFrame = _

  override def afterAll() {
    spark.stop()
  }

  case class TitleRatings(tconst: String, averageRating: Double, numVotes: Int)
  case class Top10(tconst: String, averageRating: Double, numVotes: Int, ranking: Double)
  //titleId	ordering	title	region	language	types	attributes	isOriginalTitle
  case class AllTitles(titleId: String, ordering:Int, title:String,
                       region: String, language: String, types: String, attributes: String, isOriginalTitle: Int)

  case class groupedTitles(titleId: String, titles: Seq[String])

  test("Test calculate top 10") {
    import spark.implicits._
    val inputDF = Seq(
      TitleRatings("tt0000001", 5.7, 1992),
      TitleRatings("tt0000002", 5.8, 268),
      TitleRatings("tt0000003", 6.5, 1879)
    ).toDF()
    val expectedDF: DataFrame = Seq(
      Top10("tt0000001", 5.7, 1992, 5.86639111),
      Top10("tt0000003", 6.5, 1879, 6.31025575)
    ).toDF()
    //1935.5
    val actualDF = calculateTop10(inputDF)
    assertDataFrameEquals(actualDF, expectedDF)
  }

  test("Test collect all titles") {
    import spark.implicits._
    val inputDF = Seq(
      AllTitles("tt0000001", 1, "Карменсіта", "UA", "\\N", "imdbDisplay", "\\N", 0),
      AllTitles("tt0000001", 2, "Carmencita", "DE", "\\N", "\\N", "literal title", 0),
      AllTitles("tt0000001", 3, "Carmencita - spanyol tánc", "HU", "\\N", "imdbDisplay", "\\N", 0),
    ).toDF()
    val expectedDF: DataFrame = Seq(
      groupedTitles("tt0000001", Seq("Карменсіта", "Carmencita", "Carmencita - spanyol tánc")),
    ).toDF()
    val actualDF = collectAllTitles(inputDF)
    assertDataFrameEquals(actualDF, expectedDF)
  }

}