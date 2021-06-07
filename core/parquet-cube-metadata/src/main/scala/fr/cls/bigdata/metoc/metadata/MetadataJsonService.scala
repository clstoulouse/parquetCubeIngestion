package fr.cls.bigdata.metoc.metadata

import java.io.{IOException, OutputStreamWriter, PrintWriter}

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.hadoop.concurrent.{DistributedLockingException, DistributedLockingService}
import fr.cls.bigdata.hadoop.config.DistributedLockingConfig
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.metoc.exceptions.{MetocReaderException, MetocWriterException}
import fr.cls.bigdata.metoc.metadata.diff.MetadataComparisonService
import fr.cls.bigdata.metoc.settings.DatasetSettings
import fr.cls.bigdata.metoc.service.{MetocDatasetAccess, MetocMetadataReader, MetocWriter}
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import spray.json._

import scala.io.Source

class MetadataJsonService(lockingService: DistributedLockingService,
                          hadoopConfig: Configuration,
                          comparisonService: MetadataComparisonService) extends MetocWriter[Any] with MetocMetadataReader with LazyLogging {
  /**
    * Save the metadata in a metadata.json file in the index folder of the dataset.
    * If a metadata.json file already exists, its content is compared with the current metadata.
    * If they match nothing happens, else a warning is logged and the current metadata is written to a new version of
    * the file (ex: metadata.json.1)
    *
    * @param datasetSettings The settings of the dataset
    * @param dataset     The metocReader that provides the metadata of the dataset
    * @throws MetocWriterException if an error happens
    */
  @throws[MetocWriterException]
  override def write(datasetSettings: DatasetSettings, dataset: MetocDatasetAccess[Any]): Unit = {
    write(datasetSettings, dataset.metadata)
  }

  @throws[MetocReaderException]
  override def readAll(datasetSettings: DatasetSettings): DatasetMetadata = {
    val metadataFile = MetadataJsonFile.path(datasetSettings.indexFolder)
    tryReadMetadataFile(metadataFile).getOrElse {
      throw new MetocReaderException(s"metadata file $metadataFile not found")
    }
  }

  @throws[MetocReaderException]
  override def read(datasetSettings: DatasetSettings, variables: Seq[VariableRef]): DatasetMetadata = {
    val allMetadata = readAll(datasetSettings)
    val variablesMetadata = variables.map(ref => ref -> allMetadata.variables(ref)).toMap
    val dimensionsMetadata = variablesMetadata.values.flatMap(_.dimensions).map(ref => ref -> allMetadata.dimensions(ref)).toMap
    DatasetMetadata(dimensionsMetadata, variablesMetadata, allMetadata.attributes)
  }

  private[metadata] def write(datasetSettings: DatasetSettings, metadata: DatasetMetadata): Unit = {
    val metadataFile = MetadataJsonFile.path(datasetSettings.indexFolder)
    try {
      lockingService.lock(metadataFile.path).acquire { _ =>

        val metadataToWriteIfNecessary = tryReadMetadataFile(metadataFile) match {
          case None =>
            logger.debug(s"[dataset = ${datasetSettings.name}] no existing metadata file found")
            Some(metadata)

          case Some(oldMetadata) =>
            val diff = comparisonService.compare(oldMetadata, metadata)

            if (diff.breakingChanges.nonEmpty) {
              throw new MetocWriterException(s"Breaking changes detected on dataset's metadata '${datasetSettings.name}':\n${diff.breakingChanges.mkString("\n")}")
            } else if (diff.nonBreakingChanges.nonEmpty) {
              val updatedMetadata = comparisonService.inferCommonRoot(metadata, diff.nonBreakingChanges)

              if (oldMetadata == updatedMetadata) {
                logger.info(s"[dataset = ${datasetSettings.name}] the following non-breaking metadata differences will be ignored:\n${diff.nonBreakingChanges.mkString("\n")}")
                None
              } else {
                logger.info(s"[dataset = ${datasetSettings.name}] updating the dataset metadata due to detected non-breaking differences:\n${diff.nonBreakingChanges.mkString("\n")}")
                backupMetadataFile(metadataFile)
                Some(updatedMetadata)
              }
            } else {
              logger.debug(s"[dataset = ${datasetSettings.name}] no changes detected on the metadata")
              None
            }
        }


        metadataToWriteIfNecessary.foreach { metadataToWrite =>
          logger.info(s"[dataset = ${datasetSettings.name}] writing metadata to file: $metadataFile")
          MetadataJsonService.writeMetadataFile(metadataFile, metadataToWrite)
        }
      }
    } catch {
      case cause: MetocReaderException =>
        throw new MetocWriterException(cause.getMessage, cause)
      case cause: DistributedLockingException =>
        throw new MetocWriterException(s"Failed to lock metadata file ${metadataFile.path}", cause)
      case cause: IOException =>
        throw new MetocWriterException(s"Error while writing metadata file ${metadataFile.path}", cause)
      case cause: DeserializationException =>
        throw new MetocWriterException(s"Error while de-serializing metadata", cause)
      case cause: SerializationException =>
        throw new MetocWriterException(s"Error while serializing metadata of dataset ${datasetSettings.name}", cause)
    }
  }

  private[metadata] def tryReadMetadataFile(metadataFilePath: PathWithFileSystem): Option[DatasetMetadata] = {
    try {
      if (!HadoopIO.exists(metadataFilePath)) None else {
        Some(MetadataJsonService.readMetadataFile(metadataFilePath))
      }
    } catch {
      case cause: IOException =>
        throw new MetocReaderException(s"Error while reading metadata file ${metadataFilePath.path}", cause)
      case cause: DeserializationException =>
        throw new MetocReaderException(s"Error while parsing metadata file ${metadataFilePath.path}", cause)
    }
  }

  private def backupMetadataFile(originFile: PathWithFileSystem): Unit = {
    val fileVersions = Stream.iterate(1)(_ + 1).map(_.toString)
      .map(version => originFile.map(path => new Path(s"$path.$version")))

    fileVersions.collectFirst {
      case targetFile if !HadoopIO.exists(targetFile) =>
        HadoopIO.copyOrMove(originFile, targetFile, deleteSource = true)
    }
  }
}

object MetadataJsonService {
  def writer(lockingConfig: DistributedLockingConfig, hadoopConfig: Configuration): MetocWriter[Any] = {
    val lockingService = DistributedLockingService(lockingConfig)
    new MetadataJsonService(lockingService, hadoopConfig, MetadataComparisonService)
  }

  def reader(lockingConfig: DistributedLockingConfig, hadoopConfig: Configuration): MetocMetadataReader = {
    val lockingService = DistributedLockingService(lockingConfig)
    new MetadataJsonService(lockingService, hadoopConfig, MetadataComparisonService)
  }

  @throws[IOException]
  def writeMetadataFile(filePath: PathWithFileSystem, metadata: DatasetMetadata): Unit = {
    val json = MetadataJsonFormat.write(metadata)
    for {
      outputStream <- HadoopIO.createOutputStream(filePath, overwrite = true)
      outputStreamWriter = new OutputStreamWriter(outputStream, MetadataJsonFile.codec.charSet)
      writer <- Resource(new PrintWriter(outputStreamWriter, true))
    } writer.println(json.toString())
  }

  @throws[IOException]
  def readMetadataFile(filePath: PathWithFileSystem): DatasetMetadata = {
    HadoopIO.openInputStream(filePath).acquire { stream =>
      val source = Source.fromInputStream(stream)(MetadataJsonFile.codec).getLines().mkString("\n")
      MetadataJsonFormat.read(source.parseJson)
    }
  }
}
