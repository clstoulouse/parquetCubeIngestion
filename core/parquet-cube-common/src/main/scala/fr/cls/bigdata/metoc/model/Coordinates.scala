package fr.cls.bigdata.metoc.model


/**
  * The coordinates of a position in a [[fr.cls.bigdata.metoc.model.Grid]].
  * Depth can be:
  *  - None if the coordinates are 3D
  *  - Some[Double] if the coordinates are 4D
  */
final case class Coordinates(longitude: Double, latitude: Double, time: Long, depth: Option[Double])
