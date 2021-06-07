package fr.cls.bigdata.metoc.model

import fr.cls.bigdata.georef.model.{DimensionRef, Dimensions}

import scala.collection.SortedSet

final case class Grid(longitude: SortedSet[Double],
                      latitude: SortedSet[Double],
                      time: SortedSet[Long],
                      depth: SortedSet[Double]) {
  def get(ref: DimensionRef): Seq[AnyVal] ={
    ref match {
      case Dimensions.longitude => longitude.toSeq
      case Dimensions.latitude => latitude.toSeq
      case Dimensions.time => time.toSeq
      case Dimensions.depth => depth.toSeq
    }
  }
}

object Grid {
  def create(longitude: Seq[Double], latitude: Seq[Double], time: Seq[Long], depth: Seq[Double]): Grid = Grid(
    SortedSet(longitude: _*),
    SortedSet(latitude: _*),
    SortedSet(time: _*),
    SortedSet(depth: _*)
  )
  def empty: Grid = new Grid(SortedSet(), SortedSet(), SortedSet(), SortedSet())
}
