package io.github.marnikitta.friends.application

import io.github.marnikitta.friends.{GraphCanonizer, GraphEncoder}
import org.apache.spark.{SparkConf, SparkContext}

object CompacterMain {
  def main(args: Array[String]): Unit = {
    val srcFileName =  "graph.edges"
    val dstFileName = "graph.delta"

    val conf = new SparkConf().setMaster("local[1]").setAppName("Compacter")
    val sc = new SparkContext(conf)


    val graph = sc.textFile(srcFileName)
      .map(line => {
        val edges = line.split(" ").map(_.trim.toInt)
        val a = edges(0)
        val b = edges(1)
        (a, Set(b))
      })
      .reduceByKey({ case (e1, e2) => e1 ++ e2 })
      .mapValues(_.toArray.sorted)

    val canonicalGraph = GraphCanonizer.apply(graph)

    GraphEncoder.apply(canonicalGraph).saveAsTextFile(dstFileName)
  }
}
