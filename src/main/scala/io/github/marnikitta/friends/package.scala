package io.github.marnikitta

import org.apache.spark.rdd.RDD

package object friends {
  type VertexId = Int
  type AdjList = (VertexId, Array[VertexId])
}
