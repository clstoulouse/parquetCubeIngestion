package fr.cls.bigdata.georef.model.ranges

trait GeoRange[T] {
  def lowerValue: Option[T]
  def higherValue: Option[T]
  def contains(value: T): Boolean
}
