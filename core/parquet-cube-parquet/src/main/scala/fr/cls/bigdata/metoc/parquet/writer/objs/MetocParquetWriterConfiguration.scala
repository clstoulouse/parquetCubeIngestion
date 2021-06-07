package fr.cls.bigdata.metoc.parquet.writer.objs

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.metoc.parquet.writer.partition.PartitioningStrategy
import org.apache.hadoop.conf.Configuration

/**
  * Configuration object containing the parameters of metoc parquet writer.
  *
  * @param partitioningStrategy    Partitioning strategy to use when deciding to which folder/file to write a datapoint.
  * @param fileWriterConfiguration Configuration specific to file writer.
  */
final case class MetocParquetWriterConfiguration(partitioningStrategy: PartitioningStrategy, fileWriterConfiguration: MetocParquetFileWriterConfiguration)

object MetocParquetWriterConfiguration {
  private final val path = "fr.cls.bigdata.metoc.parquet.writer"
  private final val partitioningStrategyAttribute = "partitioning-strategy"

  @throws[ConfigException]
  def apply(config: Config, hadoopConfiguration: Configuration): MetocParquetWriterConfiguration = {
    val writerConfig = config.getConfig(path)
    val partitioningStrategyName = writerConfig.getString(partitioningStrategyAttribute)
    val partitioningStrategy = PartitioningStrategy.fromName(partitioningStrategyName)
      .getOrElse {
        throw new ConfigException.BadValue(s"$path.$partitioningStrategyAttribute",
          s"Partitioning strategy $partitioningStrategyName is invalid")
      }
    val fileWriterConfiguration = MetocParquetFileWriterConfiguration(config, hadoopConfiguration)
    MetocParquetWriterConfiguration(partitioningStrategy, fileWriterConfiguration)
  }
}
