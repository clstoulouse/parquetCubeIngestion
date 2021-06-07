package fr.cls.bigdata.georef.model

trait Position

object Position {
  case class `3D`(longitude: Double, latitude: Double, time: Long)
  case class `4D`(longitude: Double, latitude: Double, time:Long, depth: Double)
}
