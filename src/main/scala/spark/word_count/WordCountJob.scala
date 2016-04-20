package spark.word_count

import org.apache.spark.SparkContext


object WordCountJob {
  def main(args: Array[String]): Unit = {
    WordCount.execute(
      master = None,
      args = args,
      jars = List(SparkContext.jarOfObject(this).get)
    )
    System.exit(0)
  }
}
