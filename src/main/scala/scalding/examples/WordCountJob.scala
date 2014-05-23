package scalding.examples

import com.twitter.scalding._
import java.net.URL

class WordCountJob(args : Args) extends Job(args) {
  TextLine(args("input"))
    .flatMap('line -> 'word) {
    line: String => tokenize(line)
  }
    .groupBy('word){_.size }
    .filter('size){size: Int => size > 4}
    .write( Tsv(args("output")))

  // split a piece of text into individual words and get the Top Level Domain
  def tokenize(text: String) = Array[String] {
    val host = new URL(text).getHost.split("\\.")
    (host.reverse(1)+"."+host.reverse(0)).toString
  }
}

object WordCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scalding.examples.WordCountJob",
    "--input", "/home/aayush/scalding_examples/src/main/resources/sampleurls.txt",
    "--output", "/home/aayush/scalding_examples/src/main/resources/wordcount",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}

