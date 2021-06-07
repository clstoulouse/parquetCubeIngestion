package fr.cls.bigdata.metoc.ingestion.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.hadoop.config.ArchiveConfiguration
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.conf.Configuration

final case class CrawlerConfiguration(shuffleDatasets: Boolean,
                                      inProgressFolderName: String,
                                      onSuccess: ArchiveConfiguration,
                                      onFailure: ArchiveConfiguration,
                                      crawlingPeriodMillis: Long,
                                      datasetReaderSettings: Map[String, DatasetReaderSettings])

object CrawlerConfiguration {
  private final val path = "fr.cls.bigdata.metoc.netcdf.crawler"
  private final val shuffleDatasetsAttribute = "shuffle-datasets"
  private final val inProgressFolderNameAttribute = "in-progress-folder-name"
  private final val onSuccessAttribute = "on-success"
  private final val onFailureAttribute = "on-failure"
  private final val crawlingPeriodAttribute = "crawling-period"
  private final val defaultRoundingAttribute = "default-rounding"
  private final val datasetsAttribute = "datasets"

  @throws[ConfigException]
  def apply(config: Config, hadoopConfig: Configuration, mode: IngestionMode): CrawlerConfiguration = {
    import scala.collection.JavaConverters._

    val crawlerConfig = config.getConfig(path)

    val shuffleDatasets = crawlerConfig.getBoolean(shuffleDatasetsAttribute)
    val inProgressFolderName = crawlerConfig.getString(inProgressFolderNameAttribute)
    val onSuccess = ArchiveConfiguration(crawlerConfig.getConfig(onSuccessAttribute), hadoopConfig)
    val onFailure = ArchiveConfiguration(crawlerConfig.getConfig(onFailureAttribute), hadoopConfig)
    val crawlingPeriodMillis = crawlerConfig.getDuration(crawlingPeriodAttribute, TimeUnit.MILLISECONDS)

    val defaultRounding = RoundingFromConfig(crawlerConfig.getConfig(defaultRoundingAttribute))

    val datasetsConfig = crawlerConfig.getConfig(datasetsAttribute)
    val datasetReaderSettings = datasetsConfig.root().keySet().asScala
      .map(name => name -> DatasetReaderSettings(datasetsConfig.getConfig(name), defaultRounding, hadoopConfig))
      .filter {case (_, settings) => settings.mode == mode }
      .toMap

    CrawlerConfiguration(
      shuffleDatasets,
      inProgressFolderName,
      onSuccess, onFailure,
      crawlingPeriodMillis,
      datasetReaderSettings
    )
  }
}
