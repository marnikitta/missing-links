package io.github.marnikitta.friends.circle

import io.github.marnikitta.friends.{AdjList, SecondCircleFinder, VertexId}
import org.apache.spark.rdd.RDD

class SecondCirclePassFriends(val limit: Int = 500) extends SecondCircleFinder {

  override def apply(in: RDD[AdjList]): RDD[(VertexId, Set[VertexId])] = {
    in.cache()

    // Collect values to memory (~8MB), then broadcast it to all workers
    val l = limit
    val possibleSecondLimit = in.mapValues(v => v.length + l).collectAsMap()

    val sendMyFriendsToKey: RDD[(VertexId, Set[VertexId])] = in
      .flatMap({ case (from, friends) => friends.map(to => (to, friends.toSet.-(to).take(possibleSecondLimit(to)))) })

    // May be it is possible to get key in reduce step, but I haven't found how
    val includeKeyToValue = sendMyFriendsToKey.map({ case (to, friends) => (to, (to, friends)) })

    // Also we can use that adj lists are sorted and union two sets by merge
    val possibleSecondFriends = includeKeyToValue
      .reduceByKey({ case ((from, friends0), (_, friends1)) => (from, (friends0 ++ friends1).take(possibleSecondLimit(from))) })
      .mapValues(_._2)

    possibleSecondFriends.join(in).mapValues({ case (possible, friends) => possible.--(friends).take(l) })
  }
}
