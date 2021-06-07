package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.{FunSpec, Matchers}

class DatasetReaderSettingsSpec extends FunSpec with Matchers {
  private val hadoopConfig = HadoopTestUtils.HadoopConfiguration
  private val defaultRounding = Rounding(precision = 4, Rounding.RoundUp)

  it(s"should throw a ${classOf[ConfigException].getSimpleName} when the config is incomplete") {
    val invalidConfig = ConfigFactory.parseString(
      """
        |{}
      """.stripMargin)
    a[ConfigException] should be thrownBy DatasetReaderSettings(invalidConfig, defaultRounding, hadoopConfig)
  }

  it(s"should parse a complete config") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  input-folder = "file:///path/to/input-folder"
        |  rounding = {
        |    coordinates-precision = 3
        |    rounding-mode = RoundDown
        |  }
        |  mode = "local"
        |  excluded-variables= ["var1", "var2"]
        |}
      """.stripMargin)

    val datasetReaderSettings = DatasetReaderSettings(config, defaultRounding, hadoopConfig)

    datasetReaderSettings.inputFolder shouldBe PathWithFileSystem("file:///path/to/input-folder", hadoopConfig)
    datasetReaderSettings.rounding shouldBe Rounding(3, Rounding.RoundDown)
    datasetReaderSettings.variablesToExclude should contain theSameElementsAs Set("var1","var2")
  }

  it(s"should default to the default rounding") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  input-folder = "file:///path/to/input-folder"
        |  mode = "local"
        |  excluded-variables= ["var1", "var2"]
        |}
      """.stripMargin)

    val datasetReaderSettings = DatasetReaderSettings(config, defaultRounding, hadoopConfig)

    datasetReaderSettings.inputFolder shouldBe PathWithFileSystem("file:///path/to/input-folder", hadoopConfig)
    datasetReaderSettings.rounding shouldBe defaultRounding
    datasetReaderSettings.variablesToExclude should contain theSameElementsAs Set("var1", "var2")
  }

  it(s"should parse a config with missing excluded-variables attribute") {
    val config = ConfigFactory.parseString(
      """
        |{
        |  input-folder = "file:///path/to/input-folder"
        |  mode = "local"
        |  rounding = {
        |    coordinates-precision = 3
        |    rounding-mode = RoundDown
        |  }
        |}
      """.stripMargin)

    val datasetReaderSettings = DatasetReaderSettings(config, defaultRounding, hadoopConfig)

    datasetReaderSettings.inputFolder shouldBe PathWithFileSystem("file:///path/to/input-folder", hadoopConfig)
    datasetReaderSettings.rounding shouldBe Rounding(3, Rounding.RoundDown)
    datasetReaderSettings.variablesToExclude shouldBe empty
  }
}
