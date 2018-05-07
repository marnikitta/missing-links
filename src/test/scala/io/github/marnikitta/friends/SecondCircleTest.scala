package io.github.marnikitta.friends

import io.github.marnikitta.friends.circle.SecondCirclePassFriends
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class SecondCircleTest extends FunSuite {
  private val conf = new SparkConf().setMaster("local[1]").setAppName("SecondCircleTest")
  private val sc = new SparkContext(conf)

  test("emptyGraph") {
    val graph = sc.parallelize(Seq[(Int, Array[Int])]())
    val result = SecondCirclePassFriends.apply(graph).collectAsMap()
    assert(result.isEmpty)
  }

  test("simpleCircle") {
    val graph = sc.parallelize(Seq((0, Array(1, 4)), (1, Array(0, 2)), (2, Array(1, 3)), (3, Array(2, 4)), (4, Array(0, 3))))
    val result = SecondCirclePassFriends.apply(graph).collectAsMap()
    assert(result == Map(0 -> Set(2, 3), 1 -> Set(3, 4), 2 -> Set(4, 0), 3 -> Set(0, 1), 4 -> Set(1, 2)))
  }

  test("star") {
    val n = 400
    val graph = sc.parallelize(Seq((0, (1 to n).toArray)) ++ (1 to n).map((_, Array(0))))
    val result = SecondCirclePassFriends.apply(graph).collectAsMap

    val exceptCenter = (1 to n).toSet
    val expected = exceptCenter.map(v => (v, exceptCenter - v)).toMap
    assert(result == expected)
  }
}
