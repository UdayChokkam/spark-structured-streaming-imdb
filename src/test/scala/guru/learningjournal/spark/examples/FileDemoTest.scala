package guru.learningjournal.spark.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import guru.learningjournal.spark.examples.FileDemo.{Credited, NameBasics, TitleAkas, TitlePrincipals, TitleRatings, Top10, calculateTop10, collectAllTitles, groupedTitles, populateCredits}

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
    val titleRatingsDF = Seq(
      TitleRatings("tt0000001", 1.0, 1000),
      TitleRatings("tt0000002", 2.0, 400),
      TitleRatings("tt0000003", 3.0, 2000)
    ).toDF
    val expectedDF = Seq(
      Top10("tt0000001", 1.0, 1000, Some(0.67)),
      Top10("tt0000003", 3.0, 2000, Some(4.0))
    ).toDF
    val actualDF = calculateTop10(titleRatingsDF)
    val actualValues = actualDF.select("ranking").collect().map(_.getDouble(0)).toList.sorted
    val expectedValues = expectedDF.select("ranking").collect().map(_.getDouble(0)).toList.sorted
    assert(expectedDF.schema == actualDF.schema)
    assert(expectedValues == actualValues)
  }

  test("Test collect all titles") {
    import spark.implicits._
    val titleAkasDF = Seq(
      TitleAkas("tt0000001", 1, "Карменсіта", "UA", "\\N", "imdbDisplay", "\\N", 0),
      TitleAkas("tt0000001", 2, "Carmencita", "DE", "\\N", "\\N", "literal title", 0),
      TitleAkas("tt0000001", 3, "Carmencita - spanyol tánc", "HU", "\\N", "imdbDisplay", "\\N", 0),
    ).toDF()
    val expectedDF: DataFrame = Seq(
      groupedTitles("tt0000001", Seq("Карменсіта", "Carmencita", "Carmencita - spanyol tánc")),
    ).toDF()
    val actualDF = collectAllTitles(titleAkasDF)
    val actualValues = actualDF.select("titles").collect().toList.flatMap(_.getSeq(0))
    val expectedValues = expectedDF.select("titles").collect().toList.flatMap(_.getSeq(0))
    assert(expectedValues == actualValues)
  }

  test("Test collect all credits") {
    import spark.implicits._

    val titlePrincipalsDF = Seq(
      TitlePrincipals("tt0000001", 1, "nm1588970", "self", "\\N", "imdbDisplay"),
      TitlePrincipals("tt0000001", 2, "nm0005690", "DE", "\\N", "\\N"),
      TitlePrincipals("tt0000001", 3, "nm0374658", "HU", "\\N", "imdbDisplay")
    ).toDF()
    val nameBasicsDF = Seq(
      NameBasics("nm1588970", "Fred Astaire", 1899, "1987", "soundtrack,actor,miscellaneous", "tt0053137,tt0072308,tt0031983,tt0050419"),
      NameBasics("nm0005690", "Lauren Bacall", 1924, "2014", "actress,soundtrack", "tt0075213,tt0038355,tt0037382,tt0117057"),
      NameBasics("nm0374658", "Brigitte Bardo", 1924, "2014", "actress,soundtrack", "tt0075213,tt0038355,tt0037382,tt0117057")
    ).toDF()
    val expectedDF: DataFrame = Seq(
      Credited("tt0000001", Seq("Fred Astaire", "Lauren Bacall", "Brigitte Bardo")),
    ).toDF()
    val actualDF = populateCredits(titlePrincipalsDF,nameBasicsDF)
    val actualValues = actualDF.select("credited").collect().toList.flatMap(_.getSeq(0))
    val expectedValues = expectedDF.select("credited").collect().toList.flatMap(_.getSeq(0))
    assert(expectedValues == actualValues)
  }

}