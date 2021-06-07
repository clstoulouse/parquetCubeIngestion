package fr.cls.bigdata.metoc.parquet.writer.objs

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.{FunSpec, Matchers}

class MetocParquetFileWriterConfigurationSpec extends FunSpec with Matchers {
  describe("companion.apply") {
    it(s"should throw an ${classOf[ConfigException].getSimpleName} when the config is invalid") {
      val invalidConfig = ConfigFactory.load("invalid.conf")
      a[ConfigException] should be thrownBy MetocParquetFileWriterConfiguration(invalidConfig, new Configuration())
    }

    it("should load reference when the config is empty") {
      val emptyConfig = ConfigFactory.load("empty.conf")
      val fileWriterConfig = MetocParquetFileWriterConfiguration(emptyConfig, new Configuration())
      fileWriterConfig.compressionCodec shouldBe CompressionCodecName.SNAPPY
    }

    it("should override reference") {
      val overrideConfig = ConfigFactory.load("override.conf")
      val fileWriterConfig = MetocParquetFileWriterConfiguration(overrideConfig, new Configuration())
      fileWriterConfig.compressionCodec shouldBe CompressionCodecName.GZIP
    }
  }
}
