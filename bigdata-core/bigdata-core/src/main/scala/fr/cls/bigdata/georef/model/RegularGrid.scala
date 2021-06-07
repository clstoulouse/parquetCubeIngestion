package fr.cls.bigdata.georef.model

case class RegularGrid private (bounds: Bounds.`3D`, resolution: Resolution) {
  val origin: Option[Position.`3D`] = {
    for {
      longitude <- bounds.lonRange.lowerValue
      latitude <- bounds.latRange.lowerValue
      time <- bounds.timeRange.lowerValue
    } yield Position.`3D`(longitude, latitude, time)
  }
}

object RegularGrid {
  def apply(resolution: Resolution, origin: Position.`3D`, bounds: Bounds.`3D`): RegularGrid = {
    RegularGrid(bounds.align(origin, resolution), resolution)
  }
}