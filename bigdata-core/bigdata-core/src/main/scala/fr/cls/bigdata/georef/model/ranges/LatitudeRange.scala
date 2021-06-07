package fr.cls.bigdata.georef.model.ranges

sealed trait LatitudeRange extends GeoRange[Double] {
  def alignWithGridInner(origin: Double, resolution: Double): LatitudeRange
}

object LatitudeRange {
  def inclusive(lower: Option[Double], higher: Option[Double]): LatitudeRange = {
    inclusive(lower.getOrElse(-90D), higher.getOrElse(90D))
  }

  def inclusive(lower: Double, higher: Double): LatitudeRange = {
    def check(value: Double): Unit = {
      if (value < -90 || value > 90)
        throw new IllegalArgumentException(s"latitude value must be in [-90, 90], found $value")
    }
    check(lower)
    check(higher)
    if (lower > higher) Empty else Inclusive(lower, higher)
  }

  case object Empty extends LatitudeRange {
    val lowerValue: Option[Double] = None
    val higherValue: Option[Double] = None
    def contains(value: Double): Boolean = false
    def alignWithGridInner(origin: Double, resolution: Double): LatitudeRange = Empty
  }

  final case class Inclusive private[ranges](lower: Double, higher: Double) extends LatitudeRange {
    val lowerValue: Option[Double] = Some(lower)
    val higherValue: Option[Double] = Some(higher)

    def contains(value: Double): Boolean = {
      lower <= value && value <= higher
    }

    def alignWithGridInner(origin: Double, resolution: Double): LatitudeRange = {
      val lowerBound = math.ceil((lower - origin) / resolution) * resolution + origin
      val higherBound = math.floor((higher - origin) / resolution) * resolution + origin
      if (lowerBound > higherBound) Empty else Inclusive(lowerBound, higherBound)
    }
  }
}
