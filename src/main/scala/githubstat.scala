import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
 * Read GitHub Event json archives from files in a folder and print out statistics of different events.
 */

object githubstat {
  def main(args: Array[String]): Unit = {
    val inputPath = "/Users/hwang/IdeaProjects/githubstat/data/"
    val dateRange = 1 to 4
    val hourRange = 0 to 23
    val eventTypes = List(
      "CommitCommentEvent",
      "CreateEvent",
      "DeleteEvent",
      "DeploymentEvent",
      "DeploymentStatusEvent",
      "DownloadEvent",
      "FollowEvent",
      "ForkEvent",
      "ForkApplyEvent",
      "GistEvent",
      "GollumEvent",
      "IssueCommentEvent",
      "IssuesEvent",
      "MemberEvent",
      "MembershipEvent",
      "PageBuildEvent",
      "PublicEvent",
      "PullRequestEvent",
      "PullRequestReviewCommentEvent",
      "PushEvent",
      "ReleaseEvent",
      "RepositoryEvent",
      "StatusEvent",
      "TeamAddEvent",
      "WatchEvent"
    )
    val eventCounts = eventTypes.map(event => (event, 0)).toMap
    val inputFileNames = for {date <- dateRange
                              hour <- hourRange} yield f"2015-01-$date%02d-$hour.json"
    val inputSources = inputFileNames.map(fileName => Source.fromFile(inputPath + fileName))
    val jsonStringsFiles = inputSources.map(_.getLines())
    for {
      jsonStrings <- jsonStringsFiles
      jsonStr <- jsonStrings
    } {
      val jsonVal = parse(jsonStr)
      for {
        JObject(eventJson) <- jsonVal
        JField("type", JString(eventType))  <- eventJson
      } {
        eventCounts(eventType) += 1
      }
    }

    // Print results.
    val maxCount = eventCounts.max
    eventCounts.foreach()

  }
}
