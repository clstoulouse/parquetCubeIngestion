package fr.cls.bigdata.metoc.ingestion.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.archive.HadoopArchiveService
import fr.cls.bigdata.hadoop.model.{AbsoluteAndRelativePath, PathWithFileSystem}
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.ingestion.config.{DatasetReaderSettings, IngestionMode}
import fr.cls.bigdata.metoc.model.Grid
import fr.cls.bigdata.metoc.service.{DataAccess, MetocDatasetAccess, MetocReader, MetocWriter}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import fr.cls.bigdata.resource.Resource
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class IngestionServiceSpec extends FunSpec with Matchers with MockFactory with HadoopTestUtils {
  val datasetName = "dataset-name"
  val datasetSettings = DatasetSettings(
    datasetName,
    PathWithFileSystem("file:///data", HadoopConfiguration),
    PathWithFileSystem("file:///index", HadoopConfiguration)
  )

  private val inputFolder = PathWithFileSystem("file:///input", HadoopConfiguration)
  private val rounding = Rounding(precision = 4, roundingMode = Rounding.RoundUp)
  private val variablesToExclude = Set.empty[String]
  private val readerSettings = DatasetReaderSettings(inputFolder, IngestionMode.Local, rounding, variablesToExclude)

  private val fileName = "file.nc"
  private val inputFile = AbsoluteAndRelativePath(inputFolder.child(fileName), fileName)
  private val datasetAccess = MetocDatasetAccess(
    fileName,
    DatasetMetadata(),
    Grid.empty,
    new DataAccess[Any] {def get: Any = null}
  )

  describe("ingest") {
    it("should archive sucessfully ingested file") {
      val writer = mock[MetocWriter[Any]]
      val reader = mock[MetocReader[Any]]
      val archivingService = mock[HadoopArchiveService]

      (reader.read _).expects(inputFile.absolutePath, rounding, variablesToExclude).returns(Resource.free(datasetAccess))
      (writer.write _).expects(datasetSettings, datasetAccess).returns()
      (archivingService.onSuccess _).expects(inputFile.absolutePath, s"$datasetName/$fileName").returns()

      val ingestionService = new IngestionService(reader, writer, archivingService)

      ingestionService.ingest(datasetSettings, readerSettings, inputFile)
    }

    it(s"should archive file whose ingestion failed") {
      val writer = mock[MetocWriter[Any]]
      val reader = mock[MetocReader[Any]]
      val archivingService = mock[HadoopArchiveService]

      (reader.read _).expects(inputFile.absolutePath, rounding, variablesToExclude).throws(new MetocReaderException(""))
      (archivingService.onFailure _).expects(inputFile.absolutePath, s"$datasetName/$fileName").returns()

      val ingestionService = new IngestionService(reader, writer, archivingService)

      ingestionService.ingest(datasetSettings, readerSettings, inputFile)
    }
  }
}
