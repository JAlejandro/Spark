package upm.eit.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object SparkApp {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("My first Spark application")
    //TODO Forcing Spark to Run in Local Mode @see http://spark.apache.org/docs/latest/submitting-applications.html
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("./src/main/resources/pagecounts_only_2000")
    val numAs = data.filter(line => line.contains("a")).count()
    val numBs = data.filter(line => line.contains("b")).count()
    println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
  }
}