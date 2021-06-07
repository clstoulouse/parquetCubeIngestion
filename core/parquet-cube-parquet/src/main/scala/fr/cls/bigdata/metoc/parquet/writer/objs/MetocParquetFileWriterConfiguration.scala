package fr.cls.bigdata.metoc.parquet.writer.objs

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Configuration object used by [[fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetFileWriter]]
  *
  * @param compressionCodec    Parquet compression codec.
  * @param hadoopConfiguration Haddop FS configuration to use when decoding path.
  */
final case class MetocParquetFileWriterConfiguration(compressionCodec: CompressionCodecName,
                                                     hadoopConfiguration: Configuration)

object MetocParquetFileWriterConfiguration {
  private final val path = "fr.cls.bigdata.metoc.parquet.writer.file-writer"
  private final val compressionCodecAttribute = "compression-codec"

  @throws[ConfigException]
  def apply(config: Config, hadoopConfiguration: Configuration): MetocParquetFileWriterConfiguration = {
    val fileWriterConfig = config.getConfig(path)

    val compressionCodecName = fileWriterConfig.getString(compressionCodecAttribute)
    val compressionCodec = try {
      CompressionCodecName.fromConf(compressionCodecName)
    } catch {
      case cause: IllegalArgumentException =>
        throw new ConfigException.BadValue(s"$path.$compressionCodecAttribute",
          s"Compression codec $compressionCodecName is invalid", cause)
    }
    MetocParquetFileWriterConfiguration(compressionCodec, hadoopConfiguration)
  }
}
