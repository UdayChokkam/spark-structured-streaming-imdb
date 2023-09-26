package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.SparkSession

object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  case class TitleRating(sno: Long, tconst: String, averageRating: Double, numVotes: Long)

  case class TitleRatingWithAverage(tconst: String, averageRating: Double, numVotes: Long, average: Double, rankingCalc: Double)

  def updateStateWithAverage(sno: Long, records: Iterator[TitleRating], state: GroupState[Map[String, TitleRatingWithAverage]]
                            ): Iterator[TitleRatingWithAverage] = {
    val mapFromState = state.getOption.getOrElse(Map.empty)
    val recordsSeq = records.toSeq
    val titleRatingWithAverages: Map[String, TitleRatingWithAverage] = recordsSeq.map(x => x.tconst -> TitleRatingWithAverage(x.tconst, x.averageRating, x.numVotes, 0.0, 0.0)).toMap
    val updated = mapFromState ++ titleRatingWithAverages
    // Calculate the sum of values and the total count
    val (sum, count) = updated.foldLeft(0.0, 0) {
      case ((s, c), (_, value)) => (s + value.numVotes, c + 1)
    }
    // Calculate the average
    val newAvg = if (count > 0) sum / count else 0.0
    val updatedAverage = updated.mapValues(x => x.copy(average = newAvg, rankingCalc = (x.numVotes / newAvg) * x.averageRating))
    val resultValues = updatedAverage.values.toSeq.sortBy(_.rankingCalc).reverse.take(2)
    state.update(updatedAverage)
    resultValues.iterator
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Imdb Streaming")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
    import spark.implicits._
    /*    val rawDF = spark.readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", "9999")
          .load()
          .as[String]

          val titleRatingsDF = rawDF
            .map(_.split("\t"))
            .map(attributes => TitleRating(1, attributes(0), attributes(1).toDouble, attributes(2).toLong))

          */

    val rawDF = spark.readStream
      .format("csv")
      .option("delimiter", "\t")
      .option("path", "inputstream")
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .load()

    // Parse the CSV data by specifying the schema
    val titleRatingsDF = rawDF
      .map(attributes => TitleRating(1, attributes(0).toString, attributes.get(1).toString.toDouble, attributes(2).toString.toLong))

    val statefulTopDF = titleRatingsDF
      .groupByKey(record => record.sno)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout()
      )(updateStateWithAverage)

    val wordCountQuery = statefulTopDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .start()
    wordCountQuery.awaitTermination()
  }
}