package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class MetocWriterTest extends FunSpec with Matchers with MockFactory with HadoopTestUtils {

  private final val datasetSettings = DatasetSettings(name = "dataset1",
    dataFolder = PathWithFileSystem("test-data-path", HadoopConfiguration),
    indexFolder = PathWithFileSystem("test-index-path", HadoopConfiguration)
  )

  describe("MetocWriter.composite") {

    it("should sequentially delegate a call to .write to its children") {
      // prepare
      val dataset = stub[MetocDatasetAccess[Any]]

      val writer1 = mock[MetocWriter[Any]]
      val writer2 = mock[MetocWriter[Any]]
      val composite = MetocWriter.composite(writer1, writer2)

      // expectations
      inSequence {
        (writer1.write _)
          .expects(datasetSettings, dataset)
          .returns()
        (writer2.write _)
          .expects(datasetSettings, dataset)
          .returns()
      }

      // perform
      composite.write(datasetSettings, dataset)
    }

    it("should raise an exception as soon as one of the children fail") {
      // prepare
      val dataset = stub[MetocDatasetAccess[Any]]

      val writer1 = mock[MetocWriter[Any]]
      val writer2 = mock[MetocWriter[Any]]
      val composite = MetocWriter.composite(writer1, writer2)
      val exception = new MetocWriterException("test")

      // expectations
      inSequence {
        (writer1.write _)
          .expects(datasetSettings, dataset)
          .throwing(exception)
      }

      // perform
      val ex = intercept[MetocWriterException] {
        composite.write(datasetSettings, dataset)
      }
      ex shouldBe exception
    }

  }
}
