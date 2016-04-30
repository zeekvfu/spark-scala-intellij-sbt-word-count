package spark.word_count

import org.apache.spark.{
SparkConf,
SparkContext
}


object WordCount {
  private val appName = "WordCountJob"

  private def tokenize(text: String): Array[String] = {
    text.replaceAll("[^0-9A-Za-z\\s]", " ").split("\\s+")
  }

  def execute(master: Option[String], args: Array[String], jars: Seq[String] = Nil): Unit = {
    val sc = {
      val conf = new SparkConf().setAppName(appName).setJars(jars)
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    val file = sc.textFile(args(0))
    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    val wordCounts = words.map(x => (x, 1)).reduceByKey((a, b) => a + b)
//    val wordCounts = words.map(x => (x, 1)).reduceByKey((a: Int, b: Int) => a + b)
    wordCounts.saveAsTextFile(args(1))
  }
}

