package fr.cls.bigdata.metoc.service

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

object DatasetRepository {
  private final val configPath = "fr.cls.bigdata.metoc.datasets"

  /**
    * Constructs a [[fr.cls.bigdata.metoc.service.DatasetRepository]] from the confguration.
    *
    * @param config App configuration.
    * @throws ConfigException If an error occurs while parsing the configuration.
    * @return An instance of [[fr.cls.bigdata.metoc.service.DatasetRepository]].
    */
  @throws[ConfigException]
  def apply(config: Config, hadoopConfig: Configuration): DatasetRepository = {
    val subConfig = config.getConfig(configPath)

    val datasets = subConfig.root().keySet().asScala
      .map(name => name -> DatasetSettings(name, subConfig.getConfig(name), hadoopConfig))
      .toMap

    DatasetRepository(datasets)
  }

  def apply(settings: (String, DatasetSettings)*): DatasetRepository = DatasetRepository(settings.toMap)
}

/**
  * Repository service that list the known datasets.
  */
case class DatasetRepository(datasetSettings: Map[String, DatasetSettings]) {
  /**
    * @param name Name of the dataset to look for.
    * @return The [[fr.cls.bigdata.metoc.settings.DatasetSettings]] corresponding to the name or `None` if the name does not match any known dataset.
    */
  def fromName(name: String): Option[DatasetSettings] = {
    datasetSettings.get(name)
  }

  /**
    * @return All the known [[fr.cls.bigdata.metoc.settings.DatasetSettings]].
    */
  def allDatasets: Iterable[DatasetSettings] = {
    datasetSettings.values
  }
}
