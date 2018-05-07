package io.github.marnikitta.friends.circle

import io.github.marnikitta.friends.metric.AdamicAdar
import io.github.marnikitta.friends.{AdjList, SecondCircleFinder, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondCirclePassFriends extends SecondCircleFinder {
  override def apply(in: RDD[AdjList]): RDD[(VertexId, Set[VertexId])] = {
    val sortedIn = in.mapValues(_.sorted).cache()

    val firstFriends: RDD[(VertexId, FriendsReducer)] = sortedIn
      .map({ case (v, friends) => (v, FriendsReducer(Some(friends.toSet + v), Set())) })

    val secondFriends: RDD[(VertexId, FriendsReducer)] = sortedIn
      .flatMap({ case (a, aFriends) => aFriends.map((_, aFriends)) })
      .mapValues(friends => FriendsReducer(None, friends.toSet))

    firstFriends.union(secondFriends)
      .reduceByKey({ case (f1, f2) => f1.reduce(f2) })
      .mapValues(_.secondFriends)
      .filter({ case (_, second) => second.nonEmpty })
  }

  case class FriendsReducer(friends: Option[Set[VertexId]], secondFriends: Set[VertexId]) {
    def reduce(that: FriendsReducer): FriendsReducer = {
      if (friends.isDefined || that.friends.isDefined) {
        val f = friends.orElse(that.friends).get
        FriendsReducer(Some(f), ((secondFriends ++ that.secondFriends) -- f).take(500))
      } else {
        FriendsReducer(Option.empty, secondFriends ++ that.secondFriends)
      }
    }
  }

}

object SecondCirclePassTriples extends SecondCircleFinder {
  override def apply(in: RDD[AdjList]): RDD[(VertexId, Set[VertexId])] = {
    val sortedIn = in.mapValues(_.sorted).cache()

    // Generate all v-shaped triples
    val vTriples: RDD[(VertexId, VertexId)] = sortedIn.flatMap({ case (middle, friends) =>
      for (i <- friends.indices; j <- friends.indices; if i < j)
        yield (friends(i), friends(j))
    }).distinct()

    val edges: RDD[(VertexId, VertexId)] = sortedIn
      .flatMap({ case (from, tos) => tos.map(to => (Math.min(from, to), Math.max(from, to))) })

    vTriples.subtract(edges)
      .flatMap({ case (a, b) => Seq((a, Set(b)), (b, Set(a))) })
      .reduceByKey({ case (s1, s2) => (s1 ++ s2).take(500) })
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("SecondCircle")
    val sc = new SparkContext(conf)

    val graph = sc.textFile("graph.edges")
      .flatMap(line => {
        val edges = line.split(" ").map(_.trim.toInt)
        val a = edges(0)
        val b = edges(1)
        Seq((a, Array(b)), (b, Array(a)))
      })
      .reduceByKey({ case (e1, e2) => e1 ++ e2 })
      .mapValues(_.sorted)
    val start = System.nanoTime()

    val secondCircle = SecondCirclePassTriples.apply(graph)

    AdamicAdar.apply(graph, secondCircle).foreach(println(_))
  }
}
