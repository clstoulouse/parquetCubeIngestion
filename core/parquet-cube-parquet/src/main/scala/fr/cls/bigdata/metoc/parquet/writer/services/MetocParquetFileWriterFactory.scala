package fr.cls.bigdata.metoc.parquet.writer.services

import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetFileWriterConfiguration
import org.apache.avro.Schema
import org.apache.hadoop.fs.Path

/**
  * Base interface that can instantiate [[fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetFileWriter]]
  */
trait MetocParquetFileWriterFactory {
  /**
    * Opens a parquet file in write mode.
    * If the file exists, it will be overwritten.
    *
    * @param parquetFilePath         Parquet file path, the path is interpreted using hadoop-fs library.
    * @param schema                  Parquet file schema.
    * @param fileWriterConfiguration Metoc parquet file writer configuration.
    * @throws MetocWriterException when an error occurs while opening the parquet file.
    * @return An instance of [[fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetFileWriter]] (this instance should be closed after usage).
    */
  @throws[MetocWriterException]
  def open(parquetFilePath: PathWithFileSystem, schema: Schema, fileWriterConfiguration: MetocParquetFileWriterConfiguration): MetocParquetFileWriter
}
