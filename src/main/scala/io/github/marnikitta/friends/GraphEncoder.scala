package io.github.marnikitta.friends

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object GraphCanonizer extends (RDD[AdjList] => RDD[AdjList]) {
  override def apply(adjLists: RDD[AdjList]): RDD[AdjList] = {
    adjLists
      .flatMapValues(a => a)
      .map({ case (from: VertexId, to: VertexId) => (Math.min(from, to), Set(Math.max(from, to))) })
      .reduceByKey({ case (a, b) => a ++ b })
      .mapValues(_.toArray.sorted)
  }
}

object GraphDecanonizer extends (RDD[AdjList] => RDD[AdjList]) {
  override def apply(adjLists: RDD[AdjList]): RDD[AdjList] = {
    adjLists
      .flatMapValues(a => a)
      .flatMap({ case (from: VertexId, to: VertexId) => Iterator((from, Set(to)), (to, Set(from))) })
      .reduceByKey({ case (a, b) => a ++ b })
      .mapValues(_.toArray.sorted)
  }
}

object GraphEncoder extends (RDD[AdjList] => RDD[String]) {
  override def apply(canonicalAdjLists: RDD[AdjList]): RDD[String] = {
    canonicalAdjLists
      .mapValues(a => {
        if (a.length > 1) {
          a.head :: a.sliding(2, 1).map(s => s(1) - s(0)).toList
        } else {
          a.toList
        }
      })
      .map({ case (from, deltaFriends) => from + deltaFriends.mkString(" ", " ", "") })
  }
}

object GraphDecoder extends (RDD[String] => RDD[AdjList]) {
  override def apply(lines: RDD[String]): RDD[AdjList] = {
    lines
      .map(line => {
        val vertices = line.split(" ")
        val from = vertices.head.toInt

        var friends = new mutable.ArrayBuilder.ofInt()
        var currentBase: Int = 0
        for (delta <- vertices.tail) {
          val nextVertex = currentBase + delta.toInt
          friends.+=(nextVertex)
          currentBase = nextVertex
        }
        (from, friends.result())
      })
  }
}

object EncoderMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("SecondCircle")
    val sc = new SparkContext(conf)

    val graph = sc.textFile("graph.edges")
      .map(line => {
        val edges = line.split(" ").map(_.trim.toInt)
        val a = edges(0)
        val b = edges(1)
        (a, Set(b))
      })
      .reduceByKey({ case (e1, e2) => e1 ++ e2 })
      .mapValues(_.toArray.sorted)

    val canonicalGraph = GraphCanonizer.apply(graph)

    GraphEncoder.apply(canonicalGraph).saveAsTextFile("graph.delta")

    val canonicalDecodedGraph = GraphDecoder.apply(sc.textFile("graph.delta"))

    assert(canonicalDecodedGraph.collectAsMap().mapValues(_.toList) == canonicalGraph.collectAsMap().mapValues(_.toList))
  }
}
