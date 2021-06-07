package fr.cls.bigdata.metoc.service

import com.typesafe.config.{ConfigException, ConfigFactory}
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.scalatest.OptionValues._
import org.scalatest.{FunSpec, Matchers}

class DatasetRepositoryTest extends FunSpec with Matchers {
  import HadoopTestUtils._

  describe("DatasetsRepository") {
    it("should list the dataset settings when the config is valid") {
      // prepare
      val config = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.metoc.datasets {
          | test-dataset-1 {
          |   data-folder = "test1"
          |   index-folder = "test2"
          | }
          | test-dataset-2 {
          |   data-folder = "test4"
          |   index-folder = "test5"
          | }
          |}
        """.stripMargin)

      // perform
      val repository = DatasetRepository(config, HadoopConfiguration)

      // checks
      repository.allDatasets.map(_.name) should contain theSameElementsAs Set("test-dataset-1", "test-dataset-2")

      val dataset1Option = repository.fromName("test-dataset-1")

      dataset1Option.value.name shouldBe "test-dataset-1"
      dataset1Option.value.dataFolder shouldBe PathWithFileSystem("test1", HadoopConfiguration)
      dataset1Option.value.indexFolder shouldBe PathWithFileSystem("test2", HadoopConfiguration)


      val dataset2Option = repository.fromName("test-dataset-2")

      dataset2Option.value.name shouldBe "test-dataset-2"
      dataset2Option.value.dataFolder shouldBe PathWithFileSystem("test4", HadoopConfiguration)
      dataset2Option.value.indexFolder shouldBe PathWithFileSystem("test5", HadoopConfiguration)
    }

    it("should list 0 datasets when run with default config") {
      // prepare
      val config = ConfigFactory.load()

      // perform
      val repository = DatasetRepository(config, HadoopTestUtils.HadoopConfiguration)

      // checks
      repository.allDatasets shouldBe empty
    }

    it("should throw ConfigException when the config is invalid") {
      // prepare
      val config = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.metoc.datasets {
          | test-dataset-1 {
          |   data-folder = "test1"
          | }
          |}
        """.stripMargin)

      // perform
      a[ConfigException] should be thrownBy DatasetRepository(config, HadoopTestUtils.HadoopConfiguration)
    }
  }

}
