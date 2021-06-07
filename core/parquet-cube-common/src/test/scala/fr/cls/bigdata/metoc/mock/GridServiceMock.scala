package fr.cls.bigdata.metoc.mock

import fr.cls.bigdata.georef.model.{Bounds, DimensionRef, Dimensions}
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.metoc.service.MetocGridService

class GridServiceMock(neighborhoods: Map[Coordinates, Seq[Coordinates]], val is3d: Boolean = true) extends MetocGridService {
  override def findGeoNeighbors(coordinates: Coordinates): Seq[Coordinates] = neighborhoods(coordinates)

  override def findClosure(box: Bounds.`3D`): Bounds.`3D` = box

  override def dimensions: Set[DimensionRef] = if (is3d) Dimensions.`3D` else Dimensions.`4D`

  override def intersection(bounds: Bounds): Grid = Grid.empty

  override def findNearest3DNeighbor(coordinates: Coordinates): Option[Coordinates] = None
}

object GridServiceMock {
  val empty3d = new GridServiceMock(Map())
  val empty4d = new GridServiceMock(Map(), false)
}
