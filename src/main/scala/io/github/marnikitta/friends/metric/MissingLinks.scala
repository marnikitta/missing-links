package io.github.marnikitta.friends.metric

import gnu.trove.map.hash.TIntDoubleHashMap
import gnu.trove.procedure.TIntDoubleProcedure
import io.github.marnikitta.friends.{AdjList, VertexId}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Searching._
import scala.collection.mutable

case class MissingLinks(localMetric: LocalMetric, secondCircleLimit: Int = 500, from: Int = 0, to: Int = Int.MaxValue) {
  def metrics(adjLists: RDD[AdjList])(implicit sc: SparkContext): RDD[(VertexId, TIntDoubleHashMap)] = {
    val in = adjLists.cache()

    // Make them local vars so that we can close them into transformations
    val localMetricClose = localMetric

    val batchFriends = in
      .filter({ case (vertex, _) => from <= vertex && vertex < to })
      .collectAsMap()
    val friendsBroadcast = sc.broadcast(batchFriends)

    val metrics = in
      .mapPartitions(tuples => {
        // val metrics = new mutable.HashMap[(VertexId, VertexId), Double]().withDefaultValue(0.0)
        val metrics = new Array[TIntDoubleHashMap](to - from)
        val offset = from

        for ((common, friends) <- tuples) {
          val iterFrom = friends.search(from).insertionPoint
          val iterTo = friends.search(to).insertionPoint

          for (iter <- iterFrom until iterTo; a = friends(iter)) {
            for (b <- friends; if b != a) {
              val metric = localMetricClose(a, common, b)

              if (metrics(a - offset) == null) {
                metrics(a - offset) = new TIntDoubleHashMap()
              }

              metrics(a - offset).adjustOrPutValue(b, metric, metric)
            }
          }
        }

        val fri = friendsBroadcast.value
        for (i <- metrics.indices) {
          metrics(i).keySet().removeAll(fri(i + offset))
        }

        metrics.toStream.zipWithIndex.map(_.swap).map({ case (a, map) => (a + offset, map) }).iterator
      })

    val reducedMetrics: RDD[(VertexId, TIntDoubleHashMap)] = if (metrics.partitions.length == 1) metrics else throw new UnsupportedOperationException("Currently only one partition is supported")

    friendsBroadcast.unpersist(blocking = false)

    reducedMetrics
  }

  def missingLinks(metrics: RDD[(VertexId, TIntDoubleHashMap)]): RDD[(VertexId, Array[(Double, VertexId)])] = {
    val secondCircleLimitClose = secondCircleLimit

    val secondCircle: RDD[(VertexId, Array[(Double, VertexId)])] = metrics.mapValues(secondCircle => {
      val tuples = new mutable.ArrayBuilder.ofRef[(Double, VertexId)]()

      secondCircle.forEachEntry(new TIntDoubleProcedure {
        override def execute(b: VertexId, metric: Double): Boolean = {
          tuples.+=((metric, b))
          true
        }
      })

      tuples.result().sorted(Ordering[(Double, VertexId)].reverse).take(secondCircleLimitClose)
    })

    val result: RDD[(VertexId, Array[(Double, VertexId)])] = if (secondCircle.partitions.length == 1) secondCircle else throw new UnsupportedOperationException("Currently only one partition is supported")

    result
  }
}
