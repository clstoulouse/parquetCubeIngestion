package fr.cls.bigdata.metoc.ingestion.job

import com.typesafe.config.ConfigException
import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.config.ConfigUtils
import fr.cls.bigdata.hadoop.archive.HadoopArchiveService
import fr.cls.bigdata.hadoop.config.{DistributedLockingConfig, HadoopConfiguration}
import fr.cls.bigdata.metoc.index.MetocIndexWriter
import fr.cls.bigdata.metoc.ingestion.config.{CrawlerConfiguration, IngestionMode}
import fr.cls.bigdata.metoc.ingestion.service.{IngestionService, NetCDFDatasetCrawler, NetCDFLocalReader}
import fr.cls.bigdata.metoc.metadata.MetadataJsonService
import fr.cls.bigdata.metoc.model.DataPoint
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetWriterConfiguration
import fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetWriter
import fr.cls.bigdata.metoc.service.{DatasetRepository, MetocWriter}

import scala.util.Random
import scala.util.control.NonFatal

object StartCrawling extends App with LazyLogging {

  try {
    val config = ConfigUtils.loadPrintAndCheckConfig(logger)

    val hadoopConfig = HadoopConfiguration(config)
    val crawlerConfig = CrawlerConfiguration(config, hadoopConfig, IngestionMode.Local)
    val writerConfig = MetocParquetWriterConfiguration(config, hadoopConfig)
    val lockingConfig = DistributedLockingConfig(config)
    val datasetsRepository = DatasetRepository(config, hadoopConfig)

    val datasetCrawlers = for {
      (datasetName, readerSettings) <- crawlerConfig.datasetReaderSettings.toSeq
      settings <- datasetsRepository.fromName(datasetName).orElse{ logger.warn(s"unknown dataset $datasetName"); None }
    } yield NetCDFDatasetCrawler(settings, readerSettings, crawlerConfig.inProgressFolderName)

    val writer = MetocWriter.composite(
      MetocParquetWriter(writerConfig),
      MetocIndexWriter(lockingConfig, hadoopConfig),
      MetadataJsonService.writer(lockingConfig, hadoopConfig)
    )

    val archivingService = HadoopArchiveService(crawlerConfig.onSuccess, crawlerConfig.onFailure)

    val ingestionService = new IngestionService(NetCDFLocalReader, writer, archivingService)

    while (true) {
      val shuffledDatasets = if (crawlerConfig.shuffleDatasets) Random.shuffle(datasetCrawlers) else datasetCrawlers
      crawl(shuffledDatasets, ingestionService)
      logger.debug(s"next crawling will start in ${crawlerConfig.crawlingPeriodMillis} millis...")
      Thread.sleep(crawlerConfig.crawlingPeriodMillis)
    }
  } catch {
    case cause: ConfigException =>
      logger.error("Exiting because an error occurred while parsing configuration", cause)
      sys.exit(1)
    case NonFatal(cause) =>
      logger.error("Exiting due to an unintended error", cause)
      sys.exit(2)
  }

  private def crawl(datasetCrawlers: Seq[NetCDFDatasetCrawler], ingestionService: IngestionService[Iterator[DataPoint]]): Unit = {
    logger.debug("start crawling...")
    for {
      crawler <- datasetCrawlers.iterator
      task <- crawler.tasks()
    } ingestionService.ingest(task.settings, task.readerSettings, task.file)
    logger.debug("ended crawling")
  }
}
