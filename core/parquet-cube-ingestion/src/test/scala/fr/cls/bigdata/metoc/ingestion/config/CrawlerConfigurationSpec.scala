package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.scalatest.OptionValues._
import org.scalatest.{FunSpec, Matchers}

class CrawlerConfigurationSpec extends FunSpec with Matchers with HadoopTestUtils {
  describe("companion.apply") {
    it(s"should throw a ${classOf[ConfigException].getSimpleName} when the config is invalid") {
      val invalidConfig = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.metoc.netcdf.crawler {
          |  shuffle-datasets = []
          |}
        """.stripMargin)
      a[ConfigException] should be thrownBy CrawlerConfiguration(invalidConfig, HadoopConfiguration, IngestionMode.Local)
    }

    it("should load reference.conf when config is empty") {
      val config = ConfigFactory.load()

      val crawlerConfig = CrawlerConfiguration(config, HadoopConfiguration, IngestionMode.Local)

      crawlerConfig.shuffleDatasets shouldBe false
      crawlerConfig.inProgressFolderName shouldBe ".inprogress"
      crawlerConfig.onSuccess.copyTo shouldBe None
      crawlerConfig.onSuccess.removeInputFile shouldBe false
      crawlerConfig.onFailure.copyTo shouldBe None
      crawlerConfig.onFailure.removeInputFile shouldBe false
      crawlerConfig.crawlingPeriodMillis shouldBe 30000
    }

    it("should load custom config") {
      val customConfig = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.metoc.netcdf.crawler {
          |  shuffle-datasets = true
          |  in-progress-folder-name = ".inprogress2"
          |  on-success {
          |   copy-to = "somewhere1"
          |   remove-input-file = true
          |  }
          |  on-failure {
          |    copy-to = "somewhere2"
          |   remove-input-file = false
          |  }
          |  crawling-period = "2 seconds"
          |  default-rounding = {
          |    coordinates-precision = 4
          |    rounding-mode = RoundDown
          |  }
          |  datasets = {
          |    test-dataset-1 {
          |      input-folder = "file:///input-folder/test-dataset-1"
          |      mode = "local"
          |      excluded-variables = [var1, var2]
          |    }
          |    test-dataset-2 {
          |      input-folder = "file:///input-folder/test-dataset-2"
          |      mode = "local"
          |      rounding = {
          |          coordinates-precision = 3
          |          rounding-mode = RoundUp
          |      }
          |    }
          |  }
          |}
          |
        """.stripMargin)

      val crawlerConfig = CrawlerConfiguration(customConfig, HadoopConfiguration, IngestionMode.Local)

      crawlerConfig.shuffleDatasets shouldBe true
      crawlerConfig.inProgressFolderName shouldBe ".inprogress2"
      crawlerConfig.onSuccess.copyTo.value shouldBe PathWithFileSystem("somewhere1", HadoopConfiguration)
      crawlerConfig.onSuccess.removeInputFile shouldBe true
      crawlerConfig.onFailure.copyTo.value shouldBe PathWithFileSystem("somewhere2", HadoopConfiguration)
      crawlerConfig.onFailure.removeInputFile shouldBe false
      crawlerConfig.crawlingPeriodMillis shouldBe 2000

      val testDataset1 = crawlerConfig.datasetReaderSettings("test-dataset-1")

      testDataset1.inputFolder shouldBe PathWithFileSystem("file:///input-folder/test-dataset-1", HadoopConfiguration)
      testDataset1.rounding shouldBe Rounding(4, Rounding.RoundDown)
      testDataset1.variablesToExclude should contain theSameElementsAs Set("var1","var2")

      val testDataset2 = crawlerConfig.datasetReaderSettings("test-dataset-2")

      testDataset2.inputFolder shouldBe PathWithFileSystem("file:///input-folder/test-dataset-2", HadoopConfiguration)
      testDataset2.rounding shouldBe Rounding(3, Rounding.RoundUp)
      testDataset2.variablesToExclude shouldBe empty
    }

    it("should load custom config with empty optional values") {
      val customConfig = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.metoc.netcdf.crawler {
          |  shuffle-datasets = true
          |  in-progress-folder-name = ".inprogress2"
          |  on-success {
          |   copy-to = ""
          |   remove-input-file = true
          |  }
          |  on-failure {
          |    copy-to = ""
          |   remove-input-file = false
          |  }
          |  crawling-period = "30 seconds"
          |  default-rounding = {
          |    coordinates-precision = 5
          |    rounding-mode = RoundUp
          |  }
          |  datasets = {}
          |}
          |
        """.stripMargin)

      val crawlerConfig = CrawlerConfiguration(customConfig, HadoopConfiguration, IngestionMode.Local)

      crawlerConfig.shuffleDatasets shouldBe true
      crawlerConfig.inProgressFolderName shouldBe ".inprogress2"
      crawlerConfig.onSuccess.copyTo shouldBe None
      crawlerConfig.onSuccess.removeInputFile shouldBe true
      crawlerConfig.onFailure.copyTo shouldBe None
      crawlerConfig.onFailure.removeInputFile shouldBe false
      crawlerConfig.crawlingPeriodMillis shouldBe 30000
    }
  }
}
