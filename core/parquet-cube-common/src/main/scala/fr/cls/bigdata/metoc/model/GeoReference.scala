package fr.cls.bigdata.metoc.model

sealed trait GeoReference

object GeoReference {
  final case object Only3D extends GeoReference
  final case object Only4D extends GeoReference
  final case object Mixed3DAnd4D extends GeoReference
}
