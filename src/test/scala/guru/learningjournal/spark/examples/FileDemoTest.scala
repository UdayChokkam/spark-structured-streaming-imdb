package guru.learningjournal.spark.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import guru.learningjournal.spark.examples.FileDemo.{calculateTop10, collectAllTitles, populateCredits}

class FileDemoTest extends FunSuite with BeforeAndAfterAll with Matchers{


  @transient val spark: SparkSession = SparkSession.builder()
    .appName("Demo Row Test")
    .master("local[3]")
    .getOrCreate()

  override def afterAll() {
    spark.stop()
  }

  test("Test calculate top 10") {
    import spark.implicits._
    val inputDF = Seq(
      TitleRatings("tt0000001", 1.0, 1000),
      TitleRatings("tt0000002", 2.0, 400),
      TitleRatings("tt0000003", 3.0, 2000)
    ).toDF()
    val expectedDF: DataFrame = Seq(
      Top10("tt0000001", 1.0, 1000, 0.66666667),
      Top10("tt0000003", 3.0, 2000, 4.0)
    ).toDF()
    val actualDF = calculateTop10(inputDF)
    val sortedActualDF = actualDF.sort("tconst")
    val sortedExpectedDF = expectedDF.sort("tconst")
    assert(expectedDF.schema == actualDF.schema)
    assert(sortedExpectedDF.collect().sameElements(sortedActualDF.collect()))
  }

  /*test("Test collect all titles") {
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

  test("Test collect all credits") {
    import spark.implicits._
    val inputDF = Seq(
      AllTitles("tt0000001", 1, "Карменсіта", "UA", "\\N", "imdbDisplay", "\\N", 0),
      AllTitles("tt0000001", 2, "Carmencita", "DE", "\\N", "\\N", "literal title", 0),
      AllTitles("tt0000001", 3, "Carmencita - spanyol tánc", "HU", "\\N", "imdbDisplay", "\\N", 0),
    ).toDF()
    val expectedDF: DataFrame = Seq(
      groupedTitles("tt0000001", Seq("Карменсіта", "Carmencita", "Carmencita - spanyol tánc")),
    ).toDF()
    val actualDF = populateCredits(inputDF)
    assertDataFrameEquals(actualDF, expectedDF)
  }*/

}