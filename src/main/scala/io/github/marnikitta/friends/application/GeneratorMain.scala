package io.github.marnikitta.friends.application

import java.io.PrintWriter
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}
import java.util

import io.github.marnikitta.friends.generation.BAGenerator

object GeneratorMain {
  def main(args: Array[String]): Unit = {
    val edgeCount = 125000000
    val vertexCount = 1250000
    val batchSize = 1000

    val graphWriter = new PrintWriter(Files.newBufferedWriter(Paths.get("graph.edges"), TRUNCATE_EXISTING, CREATE))
    val buffer = new Array[(Int, Int)](batchSize)
    var i = 0

    val degrees = new BAGenerator().generate(vertexCount, edgeCount, e => {
      if (i < batchSize) {
        buffer(i) = e
        i += 1
      } else {
        buffer.foreach(e => graphWriter.write(e._1 + " " + e._2 + "\n"))
        i = 0
      }
    })

    buffer.take(i).foreach(e => graphWriter.write(e._1 + " " + e._2 + "\n"))
    graphWriter.close()

    val degreesWriter = Files.newBufferedWriter(Paths.get("degrees.data"), CREATE_NEW)
    degreesWriter.write(util.Arrays.toString(degrees))
    degreesWriter.close()
  }
}
