package fr.cls.bigdata.parquet

import java.io.IOException

import fr.cls.bigdata.hadoop.{HadoopIO, HadoopTestUtils}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.resource.Resource
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

trait ParquetTestUtils {
  def genericRecord(schema: Schema, fields: Seq[(String, Any)]): GenericRecord = {
    val record = new GenericData.Record(schema)
    for ((name, value) <- fields) record.put(name, value)
    record
  }

  def genericRecords(schema: Schema, records: Seq[(String, Any)]*): Seq[GenericRecord] = {
    records.map(genericRecord(schema, _))
  }

  def readParquet(file: PathWithFileSystem): Seq[GenericRecord] = {
    val inputFile = HadoopInputFile.fromPath(file.path, HadoopTestUtils.HadoopConfiguration)
    val readerBuilder = AvroParquetReader.builder[GenericRecord](inputFile)
    Resource(readerBuilder.build())
      .map(reader => Iterator.continually(reader.read()).takeWhile(_ != null))
      .acquire(_.toSeq)
  }

  def readSingleParquetIn(folder: PathWithFileSystem): Seq[GenericRecord] = {
    val files = HadoopIO.listFilesInFolder(folder, recursive = true).map(_.absolutePath).toSeq
    if(files.size != 1) throw new IOException(s"More than one file found in $folder")
    readParquet(files.head)
  }

  def readAllParquetIn(folder: PathWithFileSystem): Seq[GenericRecord] = {
    HadoopIO.listFilesInFolder(folder, recursive = true).map(_.absolutePath).flatMap(readParquet).toSeq
  }
}

object ParquetTestUtils extends ParquetTestUtils
