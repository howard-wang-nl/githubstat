import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable
import scala.io.Source
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
 * Read GitHub Event json archives from files in a folder and print out statistics of different events.
 */

object githubstat {
  // Begin time label of files selected
  private val yearBegin = 2015
  private val monthBegin = 1
  private val dateBegin = 1
  private val hourBegin = 0

  // End time label of files selected
  private val yearEnd = 2015
  private val monthEnd = 2
  private val dateEnd = 28
  private val hourEnd = 23
  private val mapEventCounts = mutable.Map.empty[String, Int]

  def main(args: Array[String]): Unit = {
    val numFuture = 64 // number of Futures in a batch.
    val inputPath = "/Users/hwang/IdeaProjects/githubstat/data/"

    val files = (new java.io.File(inputPath)).listFiles
    val filesSel = files.filter(_.getName.endsWith(".json")).filter(f=>fileInDateRange(f.getName))

    for (inputFileName <- filesSel) {
      println(s"Processing $inputFileName")
      val sInput = Source.fromFile(inputFileName)
      val iLines = sInput.getLines().grouped(numFuture)
      for (lines <- iLines) {
        val sfEventTypes = lines.map(
          line => Future { // process each 7 lines in Futures (possibly the same number of threads.)
            val jsonVal = parse(line)
            val lEventType = for {
              JObject(eventJson) <- jsonVal
              JField("type", JString(eventType)) <- eventJson
            } yield eventType
            lEventType.head // take the first "type" element.
          }
        )

        // Combine Seq[Future[String]] into one Future[Seq[String]] thus able to handle all of them after completion.
       val fsEventTypes = Future.sequence(sfEventTypes)

        fsEventTypes.onComplete {
          case Success(eventTypes) => addCounter(eventTypes)
          case Failure(e) => { println("Failure in parsing json."); e.printStackTrace }
        }
        Await.ready(fsEventTypes, 10.seconds)
      }
      sInput.close() // if not calling close, there will be many files kept open in this concurrent implementation. why?
    }

    // Print results.
    mapEventCounts.foreach(m => println(m._1 + ": " + m._2))
  }

  def addCounter (eventType: Seq[String]): Unit = {
    for (et <- eventType)
      if (mapEventCounts.contains(et)) {
        mapEventCounts(et) += 1
      } else {
        mapEventCounts(et) = 1 // insert new event counter.
      }
  }

  /**
   * Check whether the time label of the file is in selected time range.
   */
  def fileInDateRange(fn: String): Boolean = {
    val fnParts = fn.split(Array('-', '.'))
    val year = fnParts(0).toInt
    val month = fnParts(1).toInt
    val date = fnParts(2).toInt
    val hour = fnParts(3).toInt

    (year > yearBegin || year == yearBegin && month > monthBegin || year == yearBegin && month == monthBegin && date > dateBegin ||
      year == yearBegin && month == monthBegin && date == dateBegin && hour >= hourBegin) &&
      (year < yearEnd || year == yearEnd && month < monthEnd || year == yearEnd && month == monthEnd && date < dateEnd ||
        year == yearEnd && month == monthEnd && date == dateEnd && hour <= hourEnd)
  }
}
