package fr.cls.bigdata.georef.model.ranges

sealed trait TimeRange extends GeoRange[Long] {
  def minus(value: Long): TimeRange
  def alignWithGridInner(origin: Long, resolution: Long): TimeRange
}

object TimeRange {
  def inclusive(lower: Option[Long], higher: Option[Long]): TimeRange = {
    inclusive(lower.getOrElse(0L), higher.getOrElse(Long.MaxValue))
  }

  def inclusive(lower: Long, higher: Long): TimeRange = {
    if (lower > higher) Empty else Inclusive(lower, higher)
  }

  case object Empty extends TimeRange {
    val lowerValue: Option[Long] = None
    val higherValue: Option[Long] = None
    def contains(value: Long): Boolean = false
    def minus(value: Long): TimeRange = Empty
    def alignWithGridInner(origin: Long, resolution: Long): TimeRange = Empty
  }

  final case class Inclusive private[ranges](lower: Long, higher: Long) extends TimeRange {
    val lowerValue: Option[Long] = Some(lower)
    val higherValue: Option[Long] = Some(higher)

    def contains(value: Long): Boolean = {
      lower <= value && value <= higher
    }

    def minus(value: Long): TimeRange = {
      Inclusive(lower - value, higher - value)
    }

    def alignWithGridInner(origin: Long, resolution: Long): TimeRange = {
      val lowerBound = math.ceil((lower - origin).toDouble / resolution.toDouble).toLong * resolution + origin
      val higherBound = math.floor((higher - origin).toDouble / resolution.toDouble).toLong * resolution + origin
      inclusive(lowerBound, higherBound)
    }
  }
}
