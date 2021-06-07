package fr.cls.bigdata.netcdf.conversion

sealed trait TimeUnit

object TimeUnit {
  final case object Hours extends TimeUnit
  final case object Days extends TimeUnit
  final case object Seconds extends TimeUnit
}
