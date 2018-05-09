package io.github.marnikitta.friends.application

import io.github.marnikitta.friends.circle.SecondCirclePassFriends
import io.github.marnikitta.friends.metric.AdamicAdar
import io.github.marnikitta.friends.{GraphDecanonizer, GraphDecoder, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MissingLinksMain {
  def main(args: Array[String]): Unit = {
    val graphFileName = "graph.delta"
    val conf = new SparkConf().setMaster("local[1]").setAppName("MissingLinks")
    val sc = new SparkContext(conf)

    val canonicalGraph = GraphDecoder.apply(sc.textFile("graph.delta"))
    val decanonizedGraph = GraphDecanonizer.apply(canonicalGraph)

    val secondCircle = new SecondCirclePassFriends().apply(decanonizedGraph)

    val missingLinkMetrics: RDD[(VertexId, Map[VertexId, Double])] = AdamicAdar.apply(decanonizedGraph, secondCircle)

    missingLinkMetrics.mapValues(m => m.toList.sortBy(-_._2)).map({ case (from, metrics) =>
      from + " " + metrics.map({ case (v, metric) => v + ":" + metric }).mkString(" ")
    }).saveAsTextFile("graph.metrics")
  }
}
