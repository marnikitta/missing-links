package io.github.marnikitta.friends.metric

import io.github.marnikitta.friends.{AdjList, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.SortedSet

class MissingLinks(val localMetric: LocalMetric, val secondCircleLimit: Int = 500) extends (RDD[AdjList] => RDD[(VertexId, Array[(VertexId, Double)])]) {
  override def apply(in: RDD[AdjList]): RDD[(VertexId, Array[(VertexId, Double)])] = {
    in.cache()

    // Make them local vars so that we can close them into transformations
    val limit = secondCircleLimit
    val lMetric = localMetric

    // Add friends count and secondCircleLimit and limit second circle possibilities with this number
    val secondLimit: Map[VertexId, Int] = in.mapValues(_.length + limit).collectAsMap().toMap

    val triples: RDD[((VertexId, VertexId), Double)] = in
      .flatMap({ case (common, arr) =>
        for (a <- arr; b <- arr; if a < b)
          yield ((a, b), lMetric(a, common, b))
      })

    val pairMetrics = triples.reduceByKey(_ + _)

    val decanonizedPairMetrics = pairMetrics.flatMap({ case ((a, b), m) => Iterator((a, (b, m)), (b, (a, m))) })

    // Add key to the value to get the key in reduce step
    val withKeyInValue = decanonizedPairMetrics.map({ case (a, tup) => (a, (a, tup)) })

    val potential: RDD[(VertexId, SortedSet[(Double, VertexId)])] = withKeyInValue
      .mapValues({ case (a, (b, metric)) =>
        val order = Ordering[(Double, VertexId)].reverse
        (a, SortedSet((metric, b))(order))
      })
      .reduceByKey({ case ((a, m1), (_, m2)) => (a, (m1 ++ m2).take(secondLimit(a))) })
      .mapValues(_._2)


    potential.join(in)
      .mapValues({ case (metrics: SortedSet[(Double, VertexId)], friends: Array[VertexId]) =>
        metrics.toStream
          .map(_.swap)
          .filter({ case (vertex, _) => !friends.contains(vertex) })
          .take(limit)
          .toArray
      })
  }
}