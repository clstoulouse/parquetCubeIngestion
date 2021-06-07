package fr.cls.bigdata.georef.model.ranges

sealed trait DepthRange extends GeoRange[Double]

object DepthRange {
  def inclusive(lower: Option[Double], higher: Option[Double]): DepthRange = {
    inclusive(lower.getOrElse(0D), higher.getOrElse(Double.MaxValue))
  }

  def inclusive(lower: Double, higher: Double): DepthRange = {
    if (lower > higher) Empty else Inclusive(lower, higher)
  }

  case object Empty extends DepthRange {
    def lowerValue: Option[Double] = None
    def higherValue: Option[Double] = None
    def contains(value: Double): Boolean = false
  }

  final case class Inclusive private[ranges](lower: Double, higher: Double) extends DepthRange {
    def lowerValue: Option[Double] = Some(lower)
    def higherValue: Option[Double] = Some(higher)
    def contains(value: Double): Boolean = {
      lower <= value && value <= higher
    }
  }
}
