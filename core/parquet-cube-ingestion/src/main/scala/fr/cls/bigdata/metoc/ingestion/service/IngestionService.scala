package fr.cls.bigdata.metoc.ingestion.service

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.archive.HadoopArchiveService
import fr.cls.bigdata.hadoop.model.{AbsoluteAndRelativePath, PathWithFileSystem}
import fr.cls.bigdata.logging.LoggingContext
import fr.cls.bigdata.metoc.exceptions.{MetocReaderException, MetocWriterException}
import fr.cls.bigdata.metoc.ingestion.config.DatasetReaderSettings
import fr.cls.bigdata.metoc.ingestion.exceptions.DatasetIngestionException
import fr.cls.bigdata.metoc.ingestion.logging.{CrawlingTracer, MdcKeys}
import fr.cls.bigdata.metoc.service.{MetocReader, MetocWriter}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.time.Timer

class IngestionService[A](reader : MetocReader[A],
                          writer: MetocWriter[A],
                          archivingService: HadoopArchiveService) extends Timer with LazyLogging {

  def ingest(settings: DatasetSettings, readerSettings: DatasetReaderSettings, inputFile: AbsoluteAndRelativePath): Unit = {
    for {
      _ <- LoggingContext(MdcKeys.DatasetName, settings.name)
      _ <- LoggingContext(MdcKeys.InputFile, inputFile.relativePath)
    } {
      try {
        CrawlingTracer.onFileStart()
        logger.info(s"Ingesting METOC input file ${inputFile.path}...")
        val elapsedTime = elapsedTimeMs {
          ingest(settings, inputFile.absolutePath, readerSettings.rounding, readerSettings.variablesToExclude)
        }
        logger.info(s"METOC successfully ingested in $elapsedTime ms for input file: ${inputFile.path}")
        archiveFile(settings, inputFile, isSuccess = true)
        CrawlingTracer.onFileSuccess(elapsedTime)
      } catch {
        case cause: DatasetIngestionException =>
          logger.error(s"An error occurred while ingesting input file: ${inputFile.path}", cause)
          archiveFile(settings, inputFile, isSuccess = false)
          CrawlingTracer.onFileFailure(cause)
      }
    }
  }

  private def archiveFile(settings: DatasetSettings, inputFile: AbsoluteAndRelativePath, isSuccess: Boolean): Unit = {
    try {
      logger.debug(s"Archiving input file: ${inputFile.path} (processed successfully = $isSuccess)...")
      if (isSuccess) archivingService.onSuccess(inputFile.absolutePath, s"${settings.name}/${inputFile.relativePath}")
      else archivingService.onFailure(inputFile.absolutePath, s"${settings.name}/${inputFile.relativePath}")
      logger.info(s"Successfully archived input file: ${inputFile.path} (processed successfully = $isSuccess)...")
    } catch {
      case t: IOException =>
        CrawlingTracer.onFileArchivingFailure(t)
        throw new DatasetIngestionException(s"[dataset = '${settings.name}'] Encountered an exception when trying to apply archive strategy on input file: ${inputFile.path}", t)
    }
  }

  private def ingest(settings: DatasetSettings, file: PathWithFileSystem, rounding: Rounding, variableToExclude: Set[String]): Unit = {
    try {
      for (dataset <- reader.read(file, rounding, variableToExclude)) {
        writer.write(settings, dataset)
      }
    } catch {
      case cause @ (_: NetCDFException | _: MetocWriterException | _: MetocReaderException | _: IOException) =>
        throw new DatasetIngestionException(s"unable to ingest file $file", cause)
    }
  }
}
