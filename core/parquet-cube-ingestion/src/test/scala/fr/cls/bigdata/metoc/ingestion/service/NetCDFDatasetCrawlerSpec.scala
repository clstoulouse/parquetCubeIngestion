package fr.cls.bigdata.metoc.ingestion.service

import java.io.IOException

import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.concurrent.ConcurrentFilePicker
import fr.cls.bigdata.hadoop.model.{AbsoluteAndRelativePath, PathWithFileSystem}
import fr.cls.bigdata.metoc.ingestion.config.{DatasetReaderSettings, IngestionMode}
import fr.cls.bigdata.metoc.ingestion.exceptions.DatasetIngestionException
import fr.cls.bigdata.metoc.ingestion.model.CrawlingTask
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class NetCDFDatasetCrawlerSpec extends FunSpec with Matchers with MockFactory with HadoopTestUtils {
  private val rounding = Rounding(2, Rounding.RoundDown)
  private val inProgressFolderName = ".inprogress"

  private val settings = DatasetSettings(
    "datasetName",
    PathWithFileSystem("dataPath", HadoopConfiguration),
    PathWithFileSystem("indexPath", HadoopConfiguration)
  )

  val file1 = AbsoluteAndRelativePath(
    PathWithFileSystem("filePath1", HadoopConfiguration),
    "relativePath1"
  )
  val file2 = AbsoluteAndRelativePath(
    PathWithFileSystem("filePath2", HadoopConfiguration),
    "relativePath2"
  )

  describe("tasks") {
    it(s"should create the input folder if it does not exist") {
      val nonExistingFolder = createTempDir().child("non-existing-dataset")
      val invalidReaderSettings = DatasetReaderSettings(nonExistingFolder, IngestionMode.Local, rounding, Set())

      val crawler = NetCDFDatasetCrawler(settings, invalidReaderSettings, inProgressFolderName)

      crawler.tasks()

      nonExistingFolder.fileSystem.isDirectory(nonExistingFolder.path) shouldBe true
    }

    it(s"should throw a ${classOf[DatasetIngestionException].getSimpleName} if the input folder is a file") {
      val invalidReaderSettings = DatasetReaderSettings(createTempFile(), IngestionMode.Local, rounding, Set())
      val crawler = NetCDFDatasetCrawler(settings, invalidReaderSettings, inProgressFolderName)
      a[DatasetIngestionException] should be thrownBy crawler.tasks()
    }

    it("should crawl two files successfully") {
      val readerSettings = DatasetReaderSettings(createTempDir(), IngestionMode.Local, rounding, Set())
      val filePicker = mock[ConcurrentFilePicker]

      inSequence {
        (filePicker.pickOneFile _).expects(true, *).returns(Some(file1))
        (filePicker.pickOneFile _).expects(true, *).returns(Some(file2))
        (filePicker.pickOneFile _).expects(true, *).returns(None)
      }

      val crawler = new NetCDFDatasetCrawler(settings, readerSettings, filePicker)
      crawler.tasks().toSeq should contain theSameElementsAs Seq(
        CrawlingTask(settings, readerSettings, file1),
        CrawlingTask(settings, readerSettings, file2)
      )
    }

    it(s"should throw an ${classOf[DatasetIngestionException].getSimpleName} if the file picker throws an ${classOf[IOException].getSimpleName}") {
      val readerSettings = DatasetReaderSettings(createTempDir(), IngestionMode.Local, rounding, Set())
      val filePicker = mock[ConcurrentFilePicker]

      (filePicker.pickOneFile _).expects(true, *).throws(new IOException())

      val crawler = new NetCDFDatasetCrawler(settings, readerSettings, filePicker)

      a[DatasetIngestionException] should be thrownBy crawler.tasks().toSeq
    }
  }
}
