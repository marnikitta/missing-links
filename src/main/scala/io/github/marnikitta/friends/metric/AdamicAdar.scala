package io.github.marnikitta.friends.metric

import io.github.marnikitta.friends.{AdjList, MetricEvaluator, VertexId}
import org.apache.spark.rdd.RDD

object AdamicAdar extends MetricEvaluator {

  override def apply(graph: RDD[AdjList], metricRequest: RDD[(VertexId, Set[VertexId])]): RDD[(VertexId, Map[VertexId, Double])] = {
    graph.cache()
    // Is collected to memory (about 8MB) to be closed into Spark job (send to each worker)
    // I guess it should work with remote master too
    val coefs = graph.mapValues(_.length).collectAsMap().mapValues(d => 1 / Math.log(d)).toMap

    val metricRequests: RDD[(VertexId, VertexId)] = graph.flatMap({ case (a, bs) => bs.map(b => (a, b)) })

    val withNeighbors: RDD[(AdjList, AdjList)] = metricRequests
      .join(graph).map({ case (a, (b, aNeighbors)) => (b, (a, aNeighbors)) })
      .join(graph).map({ case (b, (aWithN, bNeigh)) => (aWithN, (b, bNeigh)) })

    val abWithMetric: RDD[(VertexId, Map[VertexId, Double])] = withNeighbors
      .map({ case ((a, aNeigh), (b, bNeigh)) => (a, Map(b -> aNeigh.intersect(bNeigh).map(coefs(_)).sum)) })

    abWithMetric.reduceByKey((metrics1, metrics2) => metrics1 ++ metrics2)
  }
}

