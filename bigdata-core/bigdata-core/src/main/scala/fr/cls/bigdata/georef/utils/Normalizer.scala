package fr.cls.bigdata.georef.utils

trait Normalizer[A] {
  @throws[IllegalArgumentException]
  def normalize(value: A): A
}

object Normalizer {
  case class None[A]() extends Normalizer[A] {
    def normalize(value: A): A = value
  }

  case class Longitude(rounding: Rounding) extends Normalizer[Double] {
    override def normalize(lon: Double): Double = {
      rounding.round(((lon + 180D) % 360D + 360D) % 360D - 180D)
    }
  }

  case class Latitude(rounding: Rounding) extends Normalizer[Double] {
    def normalize(lat: Double): Double = {
      val roundedLat = rounding.round(lat)
      if (roundedLat < -90D || roundedLat > 90D)
        throw new IllegalArgumentException(s"Latitude value must be in [-90, 90], found $roundedLat")
      roundedLat
    }
  }
}
