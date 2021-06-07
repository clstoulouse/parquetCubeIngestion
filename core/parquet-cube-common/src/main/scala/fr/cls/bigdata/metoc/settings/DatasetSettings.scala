package fr.cls.bigdata.metoc.settings

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.conf.Configuration

/**
  * Object holding the configuration for a dataset.
  *
  * @param name      Name of the dataset.
  * @param dataFolder  Path where data is written after ingestion.
  * @param indexFolder Path where the index is written.
  */
case class DatasetSettings(name: String, dataFolder: PathWithFileSystem, indexFolder: PathWithFileSystem)

object DatasetSettings {
  /**
    * Constructs a [[fr.cls.bigdata.metoc.settings.DatasetSettings]] from a configuration portion.
    *
    * @param name      Name of the dataset.
    * @param subConfig Portion of the configuration containing the dataset settings.
    * @throws ConfigException if the configuration could not be parsed.
    * @return A constructed [[fr.cls.bigdata.metoc.settings.DatasetSettings]].
    */
  @throws[ConfigException]
  def apply(name: String, subConfig: Config, hadoopConfig: Configuration): DatasetSettings = {
    val dataFolder = subConfig.getString("data-folder")
    val indexFolder = subConfig.getString("index-folder")

    DatasetSettings(name = name,
      dataFolder = PathWithFileSystem(dataFolder, hadoopConfig),
      indexFolder = PathWithFileSystem(indexFolder, hadoopConfig))
  }
}
