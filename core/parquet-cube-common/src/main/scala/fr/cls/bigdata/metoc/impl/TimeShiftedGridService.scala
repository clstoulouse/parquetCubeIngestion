package fr.cls.bigdata.metoc.impl

import fr.cls.bigdata.georef.model.{Bounds, DimensionRef}
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.metoc.service.MetocGridService

/**
  * This implementation of MetocGridService shift the time of the inner grid according to the `timeShift` param
  *
  * @param innerGridService the original grid service of a dataset
  * @param timeShift        the shift value
  */
case class TimeShiftedGridService(innerGridService: MetocGridService, timeShift: Long) extends MetocGridService with Serializable {
  override def findNearest3DNeighbor(coordinates: Coordinates): Option[Coordinates] = {
    innerGridService.findNearest3DNeighbor(coordinates.copy(time = coordinates.time - timeShift))
      .map(neighbor => neighbor.copy(time = neighbor.time + timeShift))
  }

  override def findGeoNeighbors(coordinates: Coordinates): Seq[Coordinates] = {
    innerGridService.findGeoNeighbors(coordinates.copy(time = coordinates.time - timeShift))
      .map(neighbor => neighbor.copy(time = neighbor.time + timeShift))
  }

  override def findClosure(bounds: Bounds.`3D`): Bounds.`3D` = {
    val timeShiftedBound = bounds.shift(timeShift)
    val originalClosure = innerGridService.findClosure(timeShiftedBound)
    originalClosure.shift(-timeShift)
  }

  override def dimensions: Set[DimensionRef] = innerGridService.dimensions

  override def intersection(bounds: Bounds): Grid = {
    val timeShiftedBound = bounds.shift(timeShift)
    val intersection = innerGridService.intersection(timeShiftedBound)
    intersection.copy(time = intersection.time.map(_ + timeShift))
  }
}
