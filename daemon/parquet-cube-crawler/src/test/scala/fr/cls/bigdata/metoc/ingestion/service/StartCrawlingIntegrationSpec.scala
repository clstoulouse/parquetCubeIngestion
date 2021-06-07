package fr.cls.bigdata.metoc.ingestion.service

import com.typesafe.config.ConfigFactory
import fr.cls.bigdata.file.FileTestUtils
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.archive.HadoopArchiveService
import fr.cls.bigdata.hadoop.config.{ArchiveConfiguration, DistributedLockingConfig}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.hadoop.{HadoopIO, HadoopTestUtils}
import fr.cls.bigdata.metoc.index.{MetocIndex, MetocIndexWriter}
import fr.cls.bigdata.metoc.ingestion.config.{DatasetReaderSettings, IngestionMode}
import fr.cls.bigdata.metoc.metadata.{MetadataJsonFile, MetadataJsonService}
import fr.cls.bigdata.metoc.model.DataPoint
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetWriterConfiguration
import fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetWriter
import fr.cls.bigdata.metoc.service.MetocWriter
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.scalatest.{FunSpec, Matchers}

class StartCrawlingIntegrationSpec extends FunSpec with Matchers with HadoopTestUtils {

  private final val testFilesFolder = toHadoopPath(getClass.getResource("/netcdf").getPath)
  private final val datasetName = "dataset1"
  private final val validFileName = "dataset-alti8-nrt-global-msla-h.20190115.nc"
  private final val corruptedFileName = "corrupted.nc"
  private final val tmpFileName = ".tmp.nc"

  describe("start") {
    it("should iterate over input files and generate parquet and index for valid ones") {
      val inputFolder = createTempDir()
      fillDirectory(testFilesFolder, inputFolder)
      val dataFolder = createTempDir()
      val indexFolder = createTempDir()
      val successFolder = createTempDir()
      val failureFolder = createTempDir()

      val datasetCrawler = createDatasetCrawler(inputFolder, dataFolder, indexFolder)
      val ingestionService = createIngestionService(successFolder, failureFolder)

      for (task <- datasetCrawler.tasks()) ingestionService.ingest(task.settings, task.readerSettings, task.file)

      val partitionFolder = dataFolder.child("tspartday=2019015")

      listDirectoryContent(partitionFolder, recursive = true) should contain theSameElementsAs
        Seq(partitionFolder.child(s"dataset-alti8-nrt-global-msla-h.20190115.snappy.parquet"))
      listDirectoryContent(inputFolder, recursive = true) should contain theSameElementsAs
        Set(inputFolder.child(tmpFileName))
      listDirectoryContent(successFolder, recursive = true) should contain theSameElementsAs
        Set(successFolder.child(s"$datasetName/$validFileName"))
      listDirectoryContent(failureFolder, recursive = true) should contain theSameElementsAs
        Set(failureFolder.child(s"$datasetName/$corruptedFileName"))

      HadoopIO.exists(MetocIndex.timeFile(indexFolder)) shouldBe true
      HadoopIO.exists(MetocIndex.longitudeFile(indexFolder)) shouldBe true
      HadoopIO.exists(MetocIndex.latitudeFile(indexFolder)) shouldBe true
      HadoopIO.exists(MetadataJsonFile.path(indexFolder)) shouldBe true
    }
  }

  private def createDatasetCrawler(inputFolder: PathWithFileSystem,
                                   dataFolder: PathWithFileSystem,
                                   indexFolder: PathWithFileSystem): NetCDFDatasetCrawler = {
    val rounding = Rounding(precision = 4, Rounding.RoundUp)

    val datasetSettings = DatasetSettings(datasetName, dataFolder, indexFolder)
    val datasetReaderSettings = DatasetReaderSettings(inputFolder, IngestionMode.Spark, rounding, Set())

    NetCDFDatasetCrawler(datasetSettings, datasetReaderSettings, ".inprogress")
  }

  private def createIngestionService(successFolder: PathWithFileSystem,
                                     failureFolder: PathWithFileSystem): IngestionService[Iterator[DataPoint]] = {

    val onSuccess = ArchiveConfiguration(Some(successFolder), removeInputFile = true)
    val onFailure = ArchiveConfiguration(Some(failureFolder), removeInputFile = true)
    val archivingService = HadoopArchiveService(onSuccess, onFailure)

    val lockFolder = FileTestUtils.createTempDir()
    val lockingConfig = new DistributedLockingConfig(lockFolder, 10, 300000)
    val writerConfig = MetocParquetWriterConfiguration(ConfigFactory.load(), HadoopConfiguration)
    val writer = MetocWriter.composite(
      MetocParquetWriter(writerConfig),
      MetocIndexWriter(lockingConfig, HadoopConfiguration),
      MetadataJsonService.writer(lockingConfig, HadoopConfiguration)
    )
    new IngestionService(NetCDFLocalReader, writer, archivingService)
  }
}
