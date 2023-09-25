package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  case class TitleRating(sno: Long, tconst: String, averageRating: Double, numVotes: Long, timeSt: String)

  case class TitleRatingWithAverage(tconst: String, averageRating: Double, numVotes: Long, average: Double)

  def updateStateWithAverage(sno: Long, records: Iterator[TitleRating], state: GroupState[Map[String, TitleRatingWithAverage]]
                            ): Iterator[Map[String, TitleRatingWithAverage]] = {
    val mapFromState = state.getOption.getOrElse(Map.empty)
    val recordsSeq = records.toSeq
    val titleRatingWithAverages: Map[String, TitleRatingWithAverage] = recordsSeq.map(x => x.tconst -> TitleRatingWithAverage(x.tconst, x.averageRating, x.numVotes, 0.0)).toMap
    val updated = mapFromState ++ titleRatingWithAverages
    // Calculate the sum of values and the total count
    val (sum, count) = updated.foldLeft(0.0, 0) {
      case ((s, c), (_, value)) => (s + value.numVotes, c + 1)
    }
    // Calculate the average
    val newAvg = if (count > 0) sum / count else 0.0
    val updatedAverage = updated.mapValues(x => x.copy(average = newAvg))
    state.update(updatedAverage)
    Iterator(updatedAverage)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Imdb Streaming")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
    import spark.implicits._
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
      .as[String]
    // Define a schema for the input data
    val schema = "tconst STRING, averageRating DOUBLE, numVotes LONG"
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val currentDateTime = LocalDateTime.now()
    val formattedDateTime = currentDateTime.format(formatter)
    // Parse the CSV data by specifying the schema
    val titleRatingsDF = socketDF
      .map(_.split("\t"))
      .map(attributes => TitleRating(1, attributes(0), attributes(1).toDouble, attributes(2).toLong, LocalDateTime.now().format(formatter)))

    val statefulAvgDF = titleRatingsDF
      .groupByKey(record => record.sno)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout()
      )(updateStateWithAverage)

    val wordCountQuery = statefulAvgDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .start()
    wordCountQuery.awaitTermination()
  }
}