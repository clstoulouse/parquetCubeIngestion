package fr.cls.bigdata.georef.model.ranges

sealed trait LongitudeRange extends GeoRange[Double] {
  def alignWithGridInner(origin: Double, resolution: Double): LongitudeRange
}

object LongitudeRange {
  def inclusive(lower: Option[Double], higher: Option[Double]): LongitudeRange = {
    inclusive(lower.getOrElse(-180D), higher.getOrElse(180D))
  }

  def inclusive(lower: Double, higher: Double): LongitudeRange = {
    val normLower = normalize(lower, -180D)
    val normHigher = normalize(higher, normLower)

    if (lower > higher) Empty
    else if (higher >= lower + 360D) Inclusive(-180D, 180D)
    else if (normHigher <= 180) Inclusive(normLower, normHigher)
    else AntiMeridianInclusive(normLower, normHigher)
  }

  case object Empty extends LongitudeRange {
    val lowerValue: Option[Double] = None
    val higherValue: Option[Double] = None
    def contains(value: Double): Boolean = false
    def alignWithGridInner(origin: Double, resolution: Double): LongitudeRange = Empty
  }

  final case class Inclusive private[ranges](lower: Double, higher: Double) extends LongitudeRange {
    val lowerValue: Option[Double] = Some(lower)
    val higherValue: Option[Double] = Some(higher)

    def contains(value: Double): Boolean = {
      lower <= value && value <= higher
    }

    def alignWithGridInner(origin: Double, resolution: Double): LongitudeRange = {
      val lowerBound = math.ceil((lower - origin) / resolution) * resolution + origin
      val higherBound = math.floor((higher - origin) / resolution) * resolution + origin
      inclusive(lowerBound, higherBound)
    }
  }

  final case class AntiMeridianInclusive private[ranges](lower: Double, higher: Double) extends LongitudeRange {
    val lowerValue: Option[Double] = Some(lower)
    val higherValue: Option[Double] = Some(higher - 360D)

    def contains(value: Double): Boolean = {
      lower <= value || (value + 360D) <= higher
    }

    def alignWithGridInner(origin: Double, resolution: Double): LongitudeRange = {
      val lowerBound = math.ceil((lower - origin) / resolution) * resolution + origin
      val higherBound = math.floor((higher - origin) / resolution) * resolution + origin
      inclusive(lowerBound, higherBound)
    }
  }

  /**
    * normalize in [ref, ref + 360[
    */
  private def normalize(lon: Double, ref: Double): Double = {
    if (lon >= ref && lon < ref + 360) lon
    else {
      ((lon - ref) % 360D + 360D) % 360D + ref
    }
  }
}
