package io.github.marnikitta.friends

import org.apache.spark.rdd.RDD

import scala.collection.{SortedSet, mutable}

object GraphCanonizer extends (RDD[AdjList] => RDD[AdjList]) {
  override def apply(adjLists: RDD[AdjList]): RDD[AdjList] = {
    adjLists
      .flatMapValues(a => a)
      .map({ case (from: VertexId, to: VertexId) => (Math.min(from, to), SortedSet(Math.max(from, to))) })
      .reduceByKey(_ ++ _)
      .mapValues(_.toArray)
  }
}

object GraphDecanonizer extends (RDD[AdjList] => RDD[AdjList]) {
  override def apply(adjLists: RDD[AdjList]): RDD[AdjList] = {
    adjLists
      .flatMapValues(a => a)
      .flatMap({ case (from: VertexId, to: VertexId) => Iterator((from, SortedSet(to)), (to, SortedSet(from))) })
      .reduceByKey(_ ++_)
      .mapValues(_.toArray)
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
