package fr.cls.bigdata.metoc.parquet.writer.services

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.georef.model.{Dimensions, VariableRef}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.model.Coordinates
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetFileWriterConfiguration
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

import scala.util.control.NonFatal

private[writer] object MetocParquetFileWriter extends MetocParquetFileWriterFactory with LazyLogging {
  /**
    * Opens a parquet file in write mode.
    * If the file exists, it will be overwritten.
    *
    * @param parquetFile         Parquet file path, the path is interpreted using hadoop-fs library.
    * @param schema                  Parquet file schema.
    * @param fileWriterConfiguration Metoc parquet writer configuration.
    * @throws MetocWriterException when an error occurs while opening the parquet file.
    * @return An instance of [[fr.cls.bigdata.metoc.parquet.writer.services.MetocParquetFileWriter]] (this instance should be closed after usage).
    */
  @throws[MetocWriterException]
  override def open(parquetFile: PathWithFileSystem, schema: Schema, fileWriterConfiguration: MetocParquetFileWriterConfiguration): MetocParquetFileWriter = {
    try {
      logger.debug("creating/overwriting parquet file [compression-mode = {}]: {}", fileWriterConfiguration.compressionCodec, parquetFile)
      val parquetWriter = AvroParquetWriter.builder[GenericRecord](parquetFile.path)
        .withCompressionCodec(fileWriterConfiguration.compressionCodec)
        .withConf(fileWriterConfiguration.hadoopConfiguration)
        .withSchema(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build
      new MetocParquetFileWriter(schema, parquetWriter)
    } catch {
      case t: IOException =>
        throw new MetocWriterException(s"${classOf[IOException].getSimpleName} while writing metoc parquet file $parquetFile", t)
    }
  }

}

/**
  * Allows to write data points to a parquet file.
  *
  * @param schema        Parquet file avro schema.
  * @param parquetWriter Previously constructed [[org.apache.parquet.hadoop.ParquetWriter]] to use when writing to parquet file.
  */
private[writer] class MetocParquetFileWriter(schema: Schema, parquetWriter: ParquetWriter[GenericRecord]) {

  /**
    * writes a data point row to the underlying parquet file.
    *
    * @param coordinates Coordinates of the data point.
    * @param values   variables values (all variables declared on the schema and that
    *                    do not have a value in this object are considered `NULL`).
    * @throws MetocWriterException when an error occurs while writing to parquet file.
    */
  @throws[MetocWriterException]
  def writeDataPoint(coordinates: Coordinates, values: Seq[(VariableRef, Option[Double])]): Unit = {
    try {
      val record = new GenericData.Record(schema)
      record.put(Dimensions.time.name, coordinates.time)
      record.put(Dimensions.latitude.name, coordinates.latitude)
      record.put(Dimensions.longitude.name, coordinates.longitude)
      coordinates.depth.foreach(depth => record.put(Dimensions.depth.name, depth))
      for {
        (variable, valueOption) <- values
        value <- valueOption
      } {
        val columnName = variable.name
        record.put(columnName, value)
      }
      parquetWriter.write(record)
    } catch {
      case NonFatal(e) =>
        throw new MetocWriterException("Unable to write data point at coordinates " + coordinates, e)
    }
  }

  /**
    * Closes the underlying [[org.apache.parquet.hadoop.ParquetWriter]].
    *
    * @throws MetocWriterException when an error occurs while closing the underlying [[org.apache.parquet.hadoop.ParquetWriter]].
    */
  @throws[MetocWriterException]
  def close(): Unit = {
    try {
      parquetWriter.close()
    } catch {
      case e: IOException =>
        throw new MetocWriterException("Unable to close parquet writer", e)
    }
  }
}
