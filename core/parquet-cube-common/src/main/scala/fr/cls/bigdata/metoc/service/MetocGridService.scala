package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.georef.model.{Bounds, DimensionRef}
import fr.cls.bigdata.metoc.impl.TimeShiftedGridService
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}

/**
  * This trait encapsulates a metoc grid to offer these services:
  * - find the nearest 3d neighbors
  * - intersect with bounds
  *
  * It is serializable to be used in a Spark context
  */
trait MetocGridService extends Serializable {
  def findNearest3DNeighbor(coordinates: Coordinates): Option[Coordinates]

  /**
    * Returns the 3D neighbors in the grid of coordinates at the nearest point in time.
    * The number of returned neighbors is between 0 and 4. There is 0 neighbors if the given coordinates are outside the grid.
    * The neighbors' depth should be none.
    *
    * @param coordinates the coordinates.
    * @return a sequence of neighbors.
    */
  def findGeoNeighbors(coordinates: Coordinates): Seq[Coordinates]

  /**
    * Returns the closure of the box in the grid.
    * The result is bigger than the box to include the neighbors of the borders and corners.
    *
    * @param bounds A 3d bounding box
    * @return the closure
    */
  def findClosure(bounds: Bounds.`3D`): Bounds.`3D`

  /**
    * Returns the intersection of the grid with the bounds
    *
    * @param bounds the intersection bounds
    * @return the subset of the current grid that intersect with the provided bound
    */
  def intersection(bounds: Bounds): Grid

  /**
    * @return the set of the grid dimensions
    */
  def dimensions: Set[DimensionRef]

  /**
    * @param timeShift amount of milliseconds to shift the grid with.
    * @return A new instance of this `MetocGridService` that behave as if the grid was time shifted by `timeShift` milliseconds in the past.
    */
  def shift(timeShift: Long): MetocGridService = if(timeShift == 0L) this else TimeShiftedGridService(this, timeShift)
}
