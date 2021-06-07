package fr.cls.bigdata.metoc.ingestion.service

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.hadoop.concurrent.ConcurrentFilePicker
import fr.cls.bigdata.hadoop.model.AbsoluteAndRelativePath
import fr.cls.bigdata.logging.LoggingContext
import fr.cls.bigdata.metoc.ingestion.config.DatasetReaderSettings
import fr.cls.bigdata.metoc.ingestion.exceptions.DatasetIngestionException
import fr.cls.bigdata.metoc.ingestion.logging.MdcKeys
import fr.cls.bigdata.metoc.ingestion.model.CrawlingTask
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.commons.io.FilenameUtils

class NetCDFDatasetCrawler(settings: DatasetSettings, readerSettings: DatasetReaderSettings, filePicker: ConcurrentFilePicker) extends LazyLogging {
  private final val TemporaryFilePrefix = "."
  private final val NetCDFExtension = "nc"

  @throws[DatasetIngestionException]
  def tasks(): Iterator[CrawlingTask] = {
    LoggingContext(MdcKeys.DatasetName, settings.name).acquire { _ =>
      initInputFolder()
      logger.debug(s"Crawling input folder")
      Iterator.continually (
        try {
          filePicker.pickOneFile(recursive = true, isValid)
        } catch {
          case t: IOException =>
            throw new DatasetIngestionException(s"Could not iterate over input files", t)
        }
      ).takeWhile(_.isDefined)
        .flatten
        .map(CrawlingTask(settings, readerSettings, _))
    }
  }

  private def isValid(file: AbsoluteAndRelativePath): Boolean = {
    val name = FilenameUtils.getBaseName(file.absolutePath.toString)
    val extension = FilenameUtils.getExtension(file.absolutePath.toString)
    !name.startsWith(TemporaryFilePrefix) && extension == NetCDFExtension
  }

  @throws[DatasetIngestionException]
  private def initInputFolder(): Unit = {
    try{
      logger.debug(s"initializing input folder: ${readerSettings.inputFolder} ...")
      HadoopIO.createFolderIfNotExist(readerSettings.inputFolder)
      logger.debug(s"initialized input folder: ${readerSettings.inputFolder}")
    } catch {
      case t: IOException =>
        throw new DatasetIngestionException(s"Could not init input folder of ${settings.name}", t)
    }
  }
}

object NetCDFDatasetCrawler {
  def apply(settings: DatasetSettings, readerSettings: DatasetReaderSettings, inProgressFolderName: String): NetCDFDatasetCrawler = {
    val filePicker = ConcurrentFilePicker(readerSettings.inputFolder, inProgressFolderName)
    new NetCDFDatasetCrawler(settings, readerSettings, filePicker)
  }
}
