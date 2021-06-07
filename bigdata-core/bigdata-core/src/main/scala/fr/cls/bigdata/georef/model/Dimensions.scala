package fr.cls.bigdata.georef.model

object Dimensions {
  val longitude = DimensionRef("longitude")
  val latitude = DimensionRef("latitude")
  val time = DimensionRef("time")
  val depth = DimensionRef("depth")

  val `3D`: Set[DimensionRef] = Set(time, longitude, latitude)
  val `4D`: Set[DimensionRef] = `3D` + depth
}
