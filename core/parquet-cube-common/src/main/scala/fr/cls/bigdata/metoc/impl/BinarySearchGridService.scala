package fr.cls.bigdata.metoc.impl

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.georef.model.ranges.{GeoRange, LatitudeRange, LongitudeRange, TimeRange}
import fr.cls.bigdata.georef.model.{Bounds, DimensionRef, Dimensions}
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.metoc.service.MetocGridService

import scala.collection.SortedSet

class BinarySearchGridService(grid: Grid) extends MetocGridService with LazyLogging {
  private lazy val longitudeSeq = grid.longitude.toSeq
  private lazy val latitudeSeq = grid.latitude.toSeq
  private lazy val timeSeq = grid.time.toSeq

  import BinarySearchGridService._

  override def dimensions: Set[DimensionRef] = if (grid.depth.isEmpty) Dimensions.`3D` else Dimensions.`4D`

  override def findNearest3DNeighbor(coordinates: Coordinates): Option[Coordinates] = {
    for {
      time <- findClosest(timeSeq, coordinates.time)
      longitude <- findClosest(longitudeSeq, coordinates.longitude)
      latitude <- findClosest(latitudeSeq, coordinates.latitude)
    } yield Coordinates(longitude = longitude, latitude = latitude, time, None)
  }

  override def findGeoNeighbors(coordinates: Coordinates): Seq[Coordinates] = {
    logger.debug(s"Finding neighborhoods of $coordinates")

    for (
      timeNeighbor <- findClosest(timeSeq, coordinates.time).toSeq;
      longitudeNeighbor <- findCandidates(longitudeSeq, coordinates.longitude);
      latitudeNeighbor <- findCandidates(latitudeSeq, coordinates.latitude)
    ) yield Coordinates(longitude = longitudeNeighbor, latitude = latitudeNeighbor, timeNeighbor, None)
  }

  override def findClosure(bounds: Bounds.`3D`): Bounds.`3D` = {
    val Bounds.`3D`(lonRange, latRange, timeRange) = bounds
    val lonClosure = BinarySearchGridService.findClosure(lonRange, longitudeSeq)
      .map { case (lower, higher) => LongitudeRange.inclusive(lower, higher) }
      .getOrElse(LongitudeRange.Empty)

    val latClosure = BinarySearchGridService.findClosure(latRange, latitudeSeq)
      .map { case (lower, higher) => LatitudeRange.inclusive(lower, higher) }
      .getOrElse(LatitudeRange.Empty)

    val timeClosure = BinarySearchGridService.findClosure(timeRange, timeSeq)
      .map { case (lower, higher) => TimeRange.inclusive(lower, higher) }
      .getOrElse(TimeRange.Empty)

    Bounds.`3D`(lonClosure, latClosure, timeClosure)
  }

  override def intersection(bounds: Bounds): Grid = {
    bounds match {
      case Bounds.`3D`(lonRange, latRange, timeRange) => Grid(
        longitude = grid.longitude.filter(lonRange.contains),
        latitude = grid.latitude.filter(latRange.contains),
        time = grid.time.filter(timeRange.contains),
        depth = grid.depth
      )
      case Bounds.`4D`(lonRange, latRange, timeRange, depthRange) => Grid(
        longitude = grid.longitude.filter(lonRange.contains),
        latitude = grid.latitude.filter(latRange.contains),
        time = grid.time.filter(timeRange.contains),
        depth = grid.depth.filter(depthRange.contains)
      )
    }
  }
}

object BinarySearchGridService {

  import scala.math.Numeric.Implicits._
  import scala.math.Ordering.Implicits._

  private[impl] def inBetween[T: Ordering](min: Option[T], max: Option[T])(value: T): Boolean = {
    min.forall(value >= _) && max.forall(value <= _)
  }

  /**
    * Finds the closest value to `value` in `values`.
    *
    * @param value      Value to lookup.
    * @param values Ordered values.
    * @tparam T Values type.
    * @return Closest value to `value` or `None` if `value` is outside `values`.
    */
  private[impl] def findClosest[T: Numeric](values: Seq[T], value: T): Option[T] = {
    findClosure(values, value) match {
      case (Some(lower), Some(higher)) => if (value - lower < higher - value) Some(lower) else Some(higher)
      case _ => None
    }
  }

  /**
    * Same as `findClosure` but returns the bounds in a sorted set.
    *
    * @param values ordered values.
    * @param value     value to lookup.
    * @tparam T Values type.
    * @return The values of `values` immediately surrounding `value`.
    */
  private[impl] def findCandidates[T: Ordering](values: Seq[T], value: T): SortedSet[T] = {
    findClosure(values, value) match {
      case (Some(lower), Some(higher)) => SortedSet(lower, higher)
      case _ => SortedSet.empty
    }
  }

  private[impl] def findClosure[T: Ordering](range: GeoRange[T], values: Seq[T]): Option[(T, T)] = {
    for {
      lower <- range.lowerValue
      higher <- range.higherValue
      closure <- findClosure(lower, higher, values)
    } yield closure
  }

  /**
    * Finds the closure of a range `[lower, higher]` in a sequence of `values`.
    *
    * @param lower   lower value of the interval
    * @param higher  upper value of the interval
    * @param values  input values (should be ordered)
    * @tparam T Type of the values.
    * @return output range `(x, y)`
    */
  private[impl] def findClosure[T: Ordering](lower: T, higher: T, values: Seq[T]): Option[(T, T)] = {
    (findClosure(values, lower), findClosure(values, higher)) match {
      //   | .... |
      case ((None, Some(lowerBound)), (Some(higherBound), None)) => Some(lowerBound, higherBound)

      // ..|.. |
      case ((Some(lowerBound), Some(_)), (Some(higherBound), None)) => Some(lowerBound, higherBound)

      // | ..|..
      case ((None, Some(lowerBound)), (Some(_), Some(higherBound))) => Some(lowerBound, higherBound)

      // ..|..|..
      case ((Some(lowerBound), Some(_)), (Some(_), Some(higherBound))) => Some(lowerBound, higherBound)

      // .... | |
      case ((Some(_), None), _) => None

      // | | ....
      case (_, (None, Some(_))) => None
    }
  }

  /**
    * Returns the lower and higher bounds of a value inside an ordered sequence of values.
    * Returns the value twice if the sequence contains the value
    * Returns none if the value is outside the sequence.
    * This method performs a binary search. Complexity is O(log(N)) where N is the size of the sequence.
    *
    * @param values An ordered sequence of values
    * @param value  The value to bound
    * @tparam T The type of the value. It can be a numeric type or any other ordered type.
    * @return the lower and higher bounds
    */
  private[impl] def findClosure[T: Ordering](values: Seq[T], value: T): (Option[T], Option[T]) = {
    var lowerIdx = 0
    var higherIdx = values.size - 1

    (values(lowerIdx), values(higherIdx)) match {
      case (`value`, _) | (_, `value`) => (Some(value), Some(value))
      case (lowestValue, _) if lowestValue > value => (None, Some(lowestValue))
      case (_, highestValue) if highestValue < value => (Some(highestValue), None)
      case _ =>
        while (higherIdx - lowerIdx > 1) {
          val middleIdx = (higherIdx + lowerIdx) / 2
          val middleValue = values(middleIdx)
          if (middleValue < value) {
            lowerIdx = middleIdx
          } else if (middleValue > value) {
            higherIdx = middleIdx
          } else {
            lowerIdx = middleIdx
            higherIdx = middleIdx
          }
        }
        (Some(values(lowerIdx)), Some(values(higherIdx)))
    }
  }
}


