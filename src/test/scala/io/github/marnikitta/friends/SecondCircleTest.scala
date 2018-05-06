package io.github.marnikitta.friends

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class SecondCircleTest extends FunSuite {
  private val conf = new SparkConf().setMaster("local[1]").setAppName("SecondCircleTest")
  private val sc = new SparkContext(conf)

  test("emptyGraph") {
    val graph = toGraph(Seq())
    val result = SecondCircle.apply(graph).vertices.collectAsMap
    assert(result.isEmpty)
  }

  test("simpleCircle") {
    val graph = toGraph(Seq((0, 1), (1, 2), (2, 3), (3, 4), (4, 0)))
    val result = SecondCircle.apply(graph).vertices.collectAsMap
    assert(result == Map(0 -> Set(2, 3), 1 -> Set(3, 4), 2 -> Set(4, 0), 3 -> Set(0, 1), 4 -> Set(1, 2)))
  }

  test("star") {
    val n = 400
    val graph = toGraph((1 to n).map((0, _)))
    val result = SecondCircle.apply(graph).vertices.collectAsMap

    val exceptCenter = (1 to n).toSet
    val expected = exceptCenter.map(v => (v, exceptCenter - v)).toMap + (0 -> Set())
    assert(result == expected)
  }

  def toGraph(edSeq: Seq[(Int, Int)]): Graph[Int, Int] = {
    val vertSet: Set[Int] = edSeq.flatMap(a => Seq(a._1, a._2)).toSet
    val vertices = sc.parallelize(vertSet.map(n => (n.toLong, n)).toSeq)
    val edges = sc.parallelize(edSeq.map(e => Edge(e._1, e._2, 1)))

    Graph(vertices, edges)
  }
}
