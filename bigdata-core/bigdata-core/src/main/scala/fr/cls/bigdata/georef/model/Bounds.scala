package fr.cls.bigdata.georef.model

import fr.cls.bigdata.georef.model.ranges.{DepthRange, LatitudeRange, LongitudeRange, TimeRange}

sealed trait Bounds {
  /**
    * Shifts the time range of the current bounds by `timeShift` milliseconds in the past.
    *
    * @param timeShift Amount of milliseconds to shift this bounds by to the past.
    * @return A copy in which the time bounds are shifted.
    */
  def shift(timeShift: Long): Bounds
  def contains(position: Position.`3D`): Boolean
  def lonRange: LongitudeRange
}

object Bounds {
  def apply(minLon: Double, maxLon: Double, minLat: Double, maxLat: Double, minTime: Long, maxTime: Long): Bounds.`3D` = {
    Bounds.`3D`(
      LongitudeRange.inclusive(minLon, maxLon),
      LatitudeRange.inclusive(minLat, maxLat),
      TimeRange.inclusive(minTime, maxTime)
    )
  }

  def apply(minLon: Double, maxLon: Double, minLat: Double, maxLat: Double, minTime: Long, maxTime: Long,
            minDepth: Double, maxDepth: Double): Bounds.`4D` = {
    Bounds.`4D`(
      LongitudeRange.inclusive(minLon, maxLon),
      LatitudeRange.inclusive(minLat, maxLat),
      TimeRange.inclusive(minTime, maxTime),
      DepthRange.inclusive(minDepth, maxDepth)
    )
  }

  def apply(lonRange: LongitudeRange, latRange: LatitudeRange, timeRange: TimeRange): Bounds.`3D` = {
    Bounds.`3D`(lonRange, latRange, timeRange)
  }

  final case class `3D`(lonRange: LongitudeRange, latRange: LatitudeRange, timeRange: TimeRange) extends Bounds {
    override def shift(timeShift: Long): Bounds.`3D` = copy(timeRange = timeRange.minus(timeShift))
    def contains(position: Position.`3D`): Boolean = {
      lonRange.contains(position.longitude) &&
        latRange.contains(position.latitude) &&
        timeRange.contains(position.time)
    }

    def align(origin: Position.`3D`, resolution: Resolution): Bounds.`3D` = {
      Bounds.`3D`(
        lonRange.alignWithGridInner(origin.longitude, resolution.longitude),
        latRange.alignWithGridInner(origin.latitude, resolution.latitude),
        timeRange.alignWithGridInner(origin.time, resolution.time)
      )
    }
  }

  final case class `4D`(lonRange: LongitudeRange, latRange: LatitudeRange, timeRange: TimeRange, depthRange: DepthRange) extends Bounds {
    override def shift(timeShift: Long): Bounds.`4D` = copy(timeRange = timeRange.minus(timeShift))

    def contains(position: Position.`3D`): Boolean = {
      lonRange.contains(position.longitude) &&
        latRange.contains(position.latitude) &&
        timeRange.contains(position.time)
    }

    def contains(position: Position.`4D`): Boolean = {
      lonRange.contains(position.longitude) &&
        latRange.contains(position.latitude) &&
        timeRange.contains(position.time) &&
        depthRange.contains(position.depth)
    }
  }
}
