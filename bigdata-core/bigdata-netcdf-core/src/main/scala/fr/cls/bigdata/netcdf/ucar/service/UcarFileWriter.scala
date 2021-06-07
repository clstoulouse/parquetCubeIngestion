package fr.cls.bigdata.netcdf.ucar.service

import fr.cls.bigdata.georef.model.{DataStorage, DimensionRef, VariableRef}
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.netcdf.service.NetCDFFileWriter
import fr.cls.bigdata.netcdf.ucar.internal.{DimensionWriterAccess, VariableWriterAccess}
import ucar.nc2.NetcdfFileWriter

class UcarFileWriter(writer: NetcdfFileWriter, dimensions: Map[DimensionRef, DimensionWriterAccess], variables: Map[VariableRef, VariableWriterAccess]) extends NetCDFFileWriter {

  override def writeDimension(ref: DimensionRef, data: DataStorage): Unit = {
    val access = dimensions.getOrElse(ref, throw new NetCDFException(s"unknown dimension $ref"))
    val array = access.buildArray(data, Array(access.length))
    writer.write(access.variable, array)
  }

  override def writeVariable(ref: VariableRef, chunk: DataChunk, data: DataStorage): Unit = {
    val access = variables.getOrElse(ref, throw new NetCDFException(s"unknown variable $ref"))
    val array = access.buildArray(data, chunk.dimensions.map(_.size).toArray)
    val origin = chunk.dimensions.map(_.start).toArray
    writer.write(access.variable, origin, array)
  }

  private [ucar] def file = writer.getNetcdfFile

  override def getShape(ref: VariableRef): DataShape = {
    variables.getOrElse(ref, throw new NetCDFException(s"unknown variable $ref")).shape
  }
}
