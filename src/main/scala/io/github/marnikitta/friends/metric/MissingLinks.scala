package io.github.marnikitta.friends.metric

import io.github.marnikitta.friends.{AdjList, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeSet

class MissingLinks(val localMetric: LocalMetric, val linksLimit: Int = 500) extends (RDD[AdjList] => RDD[(VertexId, Map[VertexId, Double])]) {

  override def apply(in: RDD[AdjList]): RDD[(VertexId, Map[VertexId, Double])] = {
    in.cache()

    // Make them local vars so that we can close them into transformations
    val limit = linksLimit
    val lMetric = localMetric

    val secondLimit = in.mapValues(_.length + limit).collectAsMap()

    val triples: RDD[((VertexId, VertexId), Double)] = in
      .flatMap({ case (common, arr) =>
        for (a <- arr; b <- arr; if a < b)
          yield ((a, b), lMetric(a, common, b))
      })

    val pairMetrics = triples.reduceByKey(_ + _)

    val decanonizedPairMetrics = pairMetrics.flatMap({ case ((a, b), m) => Iterator((a, (b, m)), (b, (a, m))) })

    // Add key to the value to get the key in reduce step
    val withKeyInValue = decanonizedPairMetrics.map({ case (a, tup) => (a, (a, tup)) })

    val potential = withKeyInValue
      .mapValues({ case (a, (b, metric)) => (a, TreeSet((metric, b))) })
      .reduceByKey({ case ((a, m1), (_, m2)) => (a, (m1 ++ m2).take(secondLimit(a))) })
      .mapValues(_._2)


    potential.join(in)
      .mapValues({ case (metrics: Set[(Double, VertexId)], friends: Array[VertexId]) =>
        metrics
          .map(_.swap)
          .toMap
          .--(friends)
          .take(limit)
      })
  }
}

