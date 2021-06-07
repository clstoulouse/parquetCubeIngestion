package fr.cls.bigdata.georef.model

/**
  * The Resolution class describes the size of the cells of a density map
  * along the longitude, latitude and time dimensions.
  */
case class Resolution(longitude: Double, latitude: Double, time: Long)
