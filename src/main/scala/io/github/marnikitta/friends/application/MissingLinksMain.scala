package io.github.marnikitta.friends.application

import io.github.marnikitta.friends._
import io.github.marnikitta.friends.metric.{AdamicAdar, MissingLinks}
import org.apache.spark.{SparkConf, SparkContext}

object MissingLinksMain {
  def main(args: Array[String]): Unit = {
    val graphFileName = if (args.isEmpty) "graph.delta" else args(0)
    println("Graph file name: " + graphFileName)

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("MissingLinks")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val bucketSize = 50

    val sc = new SparkContext(conf)

    val canonicalGraph = GraphDecoder.apply(sc.textFile(graphFileName))
    val decanonizedGraph = GraphDecanonizer.apply(canonicalGraph).repartition(1).cache()

    val degrees = decanonizedGraph.mapValues(_.length).collectAsMap().toMap

    val adamicAdar = AdamicAdar(degrees)

    // We assume that vertexIds are integers from range [0, vertices.length)
    val verticesCount = degrees.size

    val bucketBounds = (0 until verticesCount by bucketSize).map(left => (left, Math.min(left + bucketSize, verticesCount)))

    for ((left, right) <- bucketBounds) {
      val evaluator = MissingLinks(adamicAdar, from = left, to = right)

      val metrics = evaluator.metrics(decanonizedGraph)(sc)
      evaluator.missingLinks(metrics)
        .map({ case (from, secondCircle) =>
          from + " " + secondCircle.map(_._2).mkString(" ")
        })
        .saveAsTextFile("links/" + left + "-" + right + ".circle")

      //      metrics
      //        .flatMap(_._2.values())
      //        .saveAsTextFile("adamic/" + left + "-" + right + ".data")
    }
  }
}
