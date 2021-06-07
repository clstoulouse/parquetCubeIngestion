package fr.cls.bigdata.metoc.parquet.writer.objs

import com.typesafe.config.{ConfigException, ConfigFactory}
import fr.cls.bigdata.metoc.parquet.writer.partition.TsPartDayPartitioning
import org.apache.hadoop.conf.Configuration
import org.scalatest.{FunSpec, Matchers}

class MetocParquetWriterConfigurationSpec extends FunSpec with Matchers {
  describe("companion.apply") {
    it(s"should throw an ${classOf[ConfigException].getSimpleName} when the config is invalid") {
      val invalidConfig = ConfigFactory.load("invalid.conf")
      a[ConfigException] should be thrownBy MetocParquetWriterConfiguration(invalidConfig, new Configuration())
    }

    it("should load reference when the config is empty") {
      val emptyConfig = ConfigFactory.load("empty.conf")
      val writerConfig = MetocParquetWriterConfiguration(emptyConfig, new Configuration())
      writerConfig.partitioningStrategy shouldBe TsPartDayPartitioning
    }

    it("should override reference") {
      val overrideConfig = ConfigFactory.load("override-reference")
      val writerConfig = MetocParquetWriterConfiguration(overrideConfig, new Configuration())
      writerConfig.partitioningStrategy shouldBe TsPartDayPartitioning
    }
  }
}
