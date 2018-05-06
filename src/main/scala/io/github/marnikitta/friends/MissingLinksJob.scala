package io.github.marnikitta.friends

import org.apache.spark.{SparkConf, SparkContext}

object MissingLinksJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MissingLinks")

    val sc = new SparkContext(conf)
    sc.textFile("graph.edges").foreach(line => println(line))
  }
}
