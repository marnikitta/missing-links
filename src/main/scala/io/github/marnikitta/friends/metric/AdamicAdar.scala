package io.github.marnikitta.friends.metric

import io.github.marnikitta.friends.{AdjList, MetricEvaluator, VertexId}
import org.apache.spark.rdd.RDD

object AdamicAdar extends MetricEvaluator {

  override def apply(graph: RDD[AdjList], metricRequest: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, Map[VertexId, Double])] = {
    graph.cache()
    // Is collected to memory (about 8MB) to be closed into Spark job (send to each worker)
    // I guess it should work with remote master too
    val coefs = graph.mapValues(tos => 1 / Math.log(tos.length)).collectAsMap()

    val metricRequests: RDD[(VertexId, VertexId)] = graph.flatMap({ case (from, tos) => tos.map(to => (to, from)) })

    val withFriends: RDD[(AdjList, AdjList)] = metricRequests
      .join(graph).map({ case (to, (from, toFriends)) => (from, (to, toFriends)) })
      .join(graph).map({ case (from, (toWithFriends, fromFriends)) => ((from, fromFriends), toWithFriends) })

    val withMetric: RDD[(VertexId, Map[VertexId, Double])] = withFriends
      .map({ case ((from, fromFriends), (to, toFriends)) => (from, Map(to -> fromFriends.intersect(toFriends).map(coefs(_)).sum)) })

    withMetric.reduceByKey((metrics1, metrics2) => metrics1 ++ metrics2)
  }
}

