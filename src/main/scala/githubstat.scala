import org.json4s._
import org.json4s.native.JsonMethods._
import scala.collection.mutable
import scala.io.Source

/**
 * Read GitHub Event json archives from files in a folder and print out statistics of different events.
 */

object githubstat {
  def main(args: Array[String]): Unit = {
    val inputPath = "/Users/hwang/IdeaProjects/githubstat/data/"

    // FIXME: Set ranges of time, not complete yet.  Now the year, month, date, hour are checked seperately.
    val yearRange = 2015 to 2015
    val monthRange = 1 to 1
    val dateRange = 1 to 4
    val hourRange = 0 to 23

    val mapEventCounts = mutable.Map.empty[String, Int]
    val inputFileNames = for {
                              year <- yearRange
                              month <- monthRange
                              date <- dateRange
                              hour <- hourRange
                             } yield f"$year-$month%02d-$date%02d-$hour.json"

/* NOTE: Alternative way to get all files in a folder
val filesHere = (new java.io.File(".")).listFiles
        for (file <- filesHere)
println(file)
*/

    for (inputFileName <- inputFileNames) {
      val sInput = Source.fromFile(inputPath + inputFileName).getLines()
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
//    val maxCount = mapEventCounts.max
    mapEventCounts.foreach(m => println(m._1 + ": " + m._2))
  }
}
