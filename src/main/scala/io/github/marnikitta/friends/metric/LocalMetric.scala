package io.github.marnikitta.friends.metric

import io.github.marnikitta.friends.VertexId

trait LocalMetric extends Serializable with ((VertexId, VertexId, VertexId) => Double) {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double
}

case class AdamicAdar(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 1.0 / Math.log(degrees(common))
}

case class CommonNeighbors(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 1.0
}

case class SaltonIndex(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 1.0 / Math.sqrt(degrees(a) * degrees(b))
}

case class SorensenIndex(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 2.0 / (degrees(a) + degrees(b))
}

case class HubPromotedIndex(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 1.0 / Math.min(degrees(a), degrees(b))
}

case class LeichtHolmeNewmanIndex(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = 1.0 / (degrees(a) * degrees(b))
}

case class PreferentialAttachment(degrees: Map[VertexId, Int]) extends LocalMetric {
  override def apply(a: VertexId, common: VertexId, b: VertexId): Double = degrees(a) * degrees(b)
}

object LocalMetric {
  def combine(components: Seq[(Double, LocalMetric)]): LocalMetric = CombinedMetric(components)

  private case class CombinedMetric(components: Seq[(Double, LocalMetric)]) extends LocalMetric {
    override def apply(a: VertexId, common: VertexId, b: VertexId): Double =
      components.map({ case (coef, metric) => coef * metric.apply(a, common, b) }).sum
  }

}
