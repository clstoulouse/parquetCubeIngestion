package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.conf.Configuration

import scala.collection.convert.DecorateAsScala

/**
  * Object representing the reader settings for a dataset.
  *
  * @param inputFolder        Path to the netCDF input folder
  * @param mode               Ingestion mode of the dataset (spark or local)
  * @param rounding           Rounding mode for the coordinates.
  * @param variablesToExclude List of variables to exclude when reading the dataset.
  */
case class DatasetReaderSettings(inputFolder: PathWithFileSystem,
                                 mode: IngestionMode,
                                 rounding: Rounding,
                                 variablesToExclude: Set[String])

object DatasetReaderSettings extends DecorateAsScala {
  private final val inputFolderAttribute = "input-folder"
  private final val roundingAttribute = "rounding"
  private final val excludedVariableSetAttribute = "excluded-variables"
  private final val modeAttribute = "mode"

  /**
    * Constructs a [[DatasetReaderSettings]] from a configuration portion.
    *
    * @param config Portion of the configuration containing the dataset reader settings.
    * @throws ConfigException if the configuration could not be parsed.
    * @return A constructed [[DatasetReaderSettings]].
    */
  @throws[ConfigException]
  def apply(config: Config, defaultRounding: Rounding, hadoopConfig: Configuration): DatasetReaderSettings = {
    val inputFolder = PathWithFileSystem(config.getString(inputFolderAttribute), hadoopConfig)

    val mode = IngestionMode.fromName(config.getString(modeAttribute))

    val rounding = if (config.hasPath(roundingAttribute)) {
      RoundingFromConfig(config.getConfig(roundingAttribute))
    } else defaultRounding

    val variablesToExclude = if (config.hasPath(excludedVariableSetAttribute)) {
      config.getStringList(excludedVariableSetAttribute).asScala.toSet
    } else Set.empty[String]

    new DatasetReaderSettings(inputFolder, mode, rounding, variablesToExclude)
  }
}
