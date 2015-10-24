import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable
import scala.io.Source

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

  def main(args: Array[String]): Unit = {
    val inputPath = "/Users/hwang/IdeaProjects/githubstat/data/"

    val mapEventCounts = mutable.Map.empty[String, Int]
    val files = (new java.io.File(inputPath)).listFiles
    val filesSel = files.filter(f=>f.getName.endsWith(".json")).filter(f=>fileInDateRange(f.getName))

    for (inputFileName <- filesSel) {
      println(s"Processing $inputFileName")
      val sInput = Source.fromFile(inputFileName).getLines()
      for (line <- sInput ) {
        val jsonVal = parse(line)
        val lEventType = for {JObject(eventJson) <- jsonVal
             JField("type", JString(eventType)) <- eventJson} yield eventType
        val type1 = lEventType.head // take the first "type" element.
        if (mapEventCounts.contains(type1)) {
          mapEventCounts(type1) += 1
        } else {
          mapEventCounts(type1) = 1 // insert new event counter.
        }
      }
    }

    // Print results.
    mapEventCounts.foreach(m => println(m._1 + ": " + m._2))
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
