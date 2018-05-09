package io.github.marnikitta.friends.application

import io.github.marnikitta.friends._
import io.github.marnikitta.friends.metric.{AdamicAdar, MissingLinks}
import org.apache.spark.{SparkConf, SparkContext}

object MissingLinksMain {
  def main(args: Array[String]): Unit = {
    val graphFileName = "graph.delta"
    val conf = new SparkConf().setMaster("local[1]").setAppName("MissingLinks")
    val sc = new SparkContext(conf)

    val canonicalGraph = GraphDecoder.apply(sc.textFile("graph.delta"))
    val decanonizedGraph = GraphDecanonizer.apply(canonicalGraph)

    val degrees = decanonizedGraph.mapValues(_.length).collectAsMap().toMap

    val adamicAdar = AdamicAdar(degrees)

    new MissingLinks(adamicAdar).apply(decanonizedGraph)
      .mapValues(m => m.toList.sortBy(-_._2))
      .map({ case (from, metrics) =>
        from + " " + metrics.map({ case (v, metric) => v + ":" + metric }).mkString(" ")
      })
      .saveAsTextFile("graph.metrics")
  }
}
