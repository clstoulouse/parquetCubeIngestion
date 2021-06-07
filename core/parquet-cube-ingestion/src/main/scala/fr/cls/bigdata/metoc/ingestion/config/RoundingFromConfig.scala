package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.georef.utils.Rounding

object RoundingFromConfig {
  private final val coordinatesPrecisionAttribute = "coordinates-precision"
  private final val roundingModeAttribute = "rounding-mode"

  /**
    * Constructs a [[Rounding]] from a configuration portion.
    *
    * @param subConfig Portion of the configuration containing the dataset reader settings.
    * @throws ConfigException if the configuration could not be parsed.
    * @return A constructed [[Rounding]].
    */
  @throws[ConfigException]
  def apply(subConfig: Config): Rounding = {
    val coordinatesPrecision = subConfig.getInt(coordinatesPrecisionAttribute)
    val roundingModeName = subConfig.getString(roundingModeAttribute)
    val roundingMode = Rounding.fromName(roundingModeName).getOrElse {
      throw new ConfigException.BadValue(s"$roundingModeAttribute", s"Rounding mode $roundingModeName is invalid")
    }

    Rounding(coordinatesPrecision, roundingMode)
  }
}