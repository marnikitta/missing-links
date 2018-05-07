package io.github.marnikitta.friends.generation

import io.github.marnikitta.friends.VertexId

class BAGenerator(seed: Long) {
  val rd = new FastRandom(seed)

  def this() {
    this(System.nanoTime())
  }

  /** *
    * Generates scale-free graph using a a preferential attachment mechanism
    *
    * @param vertexCount  number of vertices in the result graph
    * @param edgeCount    approximate number of edges in the result graph
    * @param edgeConsumer consumer of created links, rare duplicates are possible
    * @return an array of vertices degrees, size of the array equals to the vertexCount
    */
  def generate(vertexCount: Int, edgeCount: Int, edgeConsumer: ((VertexId, VertexId)) => Unit): Array[Int] = {
    require(edgeCount > vertexCount, {
      "Expected number of edges should be greater then the number of vertices"
    })
    val edgesPerConnection = edgeCount / vertexCount

    // Initiate graph with two connected vertexes
    val vertexDegrees = new Array[Int](vertexCount)
    vertexDegrees(0) = 1
    vertexDegrees(1) = 1
    edgeConsumer((0, 1))

    // Run BA with m = 2 until the required number of vertexes reached
    BA(edgesPerConnection, 2, 2, vertexDegrees, edgeConsumer)

    BA(vertexCount, edgesPerConnection, edgesPerConnection, vertexDegrees, edgeConsumer)

    vertexDegrees
  }

  /** *
    *
    * @param vertexCount   number of vertices in the result graph
    * @param m0            seed subgraph size (corresponds to m_0 from wiki article)
    * @param m             number of connections of the new node (corresponds to m from wiki article)
    * @param vertexDegrees vertices degrees of the seed graph
    * @param edgeConsumer  consumer of the generated edges
    * @see <a href="https://en.wikipedia.org/wiki/Barab%C3%A1si%E2%80%93Albert_model">Barabási–Albert model</a>
    */
  private def BA(vertexCount: Int, m0: Int, m: Int, vertexDegrees: Array[Int], edgeConsumer: ((VertexId, VertexId)) => Unit): Unit = {
    require(m <= m0)
    var maxDegree = vertexDegrees.max

    for (newVertex <- m0 until vertexCount) {
      var connected = 0
      while (connected < m) {
        val potentialVertex = rd.nextInt(newVertex)
        val roulette = rd.nextInt(maxDegree)
        if (roulette < vertexDegrees(potentialVertex)) {
          connected += 1
          vertexDegrees(newVertex) += 1
          vertexDegrees(potentialVertex) += 1
          edgeConsumer((newVertex, potentialVertex))

          if (vertexDegrees(potentialVertex) > maxDegree) {
            maxDegree = vertexDegrees(potentialVertex)
          }
        }
      }
    }
  }
}
