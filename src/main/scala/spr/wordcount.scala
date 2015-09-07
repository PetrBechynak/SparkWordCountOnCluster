package spr

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val conf = new SparkConf()//.setJars(Seq("H:\\SourceTreeRepos\\wc\\wc-1.0-SNAPSHOT.jar"))
      .setMaster(master)
      .setAppName("Simple Application")
      //.set("spark.executor.memory", "1500M")
      //.set("spark.storage.memoryFraction", "0.5")
      //.set("executor-memory","1500M")
      //.set("driver-memory","1500M")

    val sc = new SparkContext(conf)
    //val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME")
    val input = args.length match {
      case x: Int if x > 1 => sc.textFile(args(1))
      case _ => sc.parallelize(List("pandas", "i like pandas"))
    }
    val words = input.flatMap(line => line.split(" "))
    words.foreach(println)
    args.length match {
      case x: Int if x > 2 => {
        val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
        counts.saveAsTextFile(args(2))
      }
      case _ => {
        val wc = words.countByValue()
        println(wc.mkString(","))
      }
    }
  }
}

