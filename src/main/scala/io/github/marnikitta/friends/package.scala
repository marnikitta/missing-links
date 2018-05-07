package io.github.marnikitta

import org.apache.spark.rdd.RDD

package object friends {
  type VertexId = Int
  type AdjList = (VertexId, Array[VertexId])
  type SecondCircleFinder = RDD[AdjList] => RDD[(VertexId, Set[VertexId])]
  type MetricEvaluator = (RDD[AdjList], RDD[(VertexId, Set[VertexId])]) => RDD[(VertexId, Map[VertexId, Double])]
}
