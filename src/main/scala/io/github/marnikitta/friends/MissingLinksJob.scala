package io.github.marnikitta.friends

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object MissingLinksJob {

  case class Dists(dists: Map[VertexId, Int] = Map()) {
    def merge(that: Dists): Dists = {
      val thisWithDef = dists.withDefaultValue(Int.MaxValue)
      val thatWithDef = that.dists.withDefaultValue(Int.MaxValue)
      val keys = thisWithDef.keys.toSet.union(thatWithDef.keys.toSet)
      val updatedDists = keys.map(k => k -> Math.min(thisWithDef(k), thatWithDef(k))).toMap

      assert(updatedDists.values.max <= 2)

      Dists(updatedDists)
    }

    def inc(): Dists = Dists(dists.mapValues(_ + 1))

    def secondCircle(): Set[VertexId] = dists.filter(_._2 == 2).keySet.take(500)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MissingLinks")
    val sc = new SparkContext(conf)

    GraphLoader.edgeListFile(sc, "graph.edges")
      .mapVertices((id, vd) => Dists(Map(id -> 0)))
      .pregel(Dists(), 2)(
        (id, vd, msg) => vd.merge(msg.inc()),
        edge => List((edge.dstId, edge.srcAttr), (edge.srcId, edge.dstAttr)).iterator,
        (msg0: Dists, msg1: Dists) => msg0.merge(msg1)
      )
      .mapVertices((id, vd) => vd.secondCircle())
      .vertices.foreach(println(_))
  }
}
