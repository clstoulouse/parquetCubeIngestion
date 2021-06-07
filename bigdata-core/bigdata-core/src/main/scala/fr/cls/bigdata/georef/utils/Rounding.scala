package fr.cls.bigdata.georef.utils

import fr.cls.bigdata.georef.utils.Rounding.Mode
import org.apache.commons.math3.util.Precision

/**
  * Helper class that implements rounding a double numeric value based on a coordinates precision and a [[fr.cls.bigdata.georef.utils.Rounding.Mode]].
  */
final case class Rounding(precision: Int, roundingMode: Mode) {

  /**
    * Rounds a value based based on a coordinates Precision and a [[fr.cls.bigdata.georef.utils.Rounding.Mode]].
    *
    * @param value Value to round.
    * @return Rounded value.
    */
  def round(value: Double): Double = {
    Precision.round(value, precision, roundingMode.id)
  }
}

object Rounding extends Enumeration {
  type Mode = Value
  val RoundUp, RoundDown, RoundCeiling, RoundFloor, RoundHalfUp, RoundHalfDown, RoundHalfEven, RoundUnnecessary = Value

  def fromName(s: String): Option[Mode] = values.find(_.toString == s)
}

