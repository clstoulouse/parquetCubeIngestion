package fr.cls.bigdata.netcdf.service

import fr.cls.bigdata.georef.model.{DataStorage, DimensionRef, VariableRef}
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape}
import fr.cls.bigdata.netcdf.exception.NetCDFException

trait NetCDFFileWriter {
  /**
    * Returns the shape of a variable from which we obtain the chunks
    *
    * @param ref the reference of the variable.
    */
  def getShape(ref: VariableRef): DataShape

  /**
    * Writes the dimension values to the NetCDF file.
    *
    * @param ref the reference of the dimension
    * @param data the values of the dimension with correct size and type
    * @throws NetCDFFileWriter when an error occur
    */
  @throws[NetCDFException]
  def writeDimension(ref: DimensionRef, data: DataStorage)

  /**
    * Writes the values of a chunk of a variable in the NetCDF file. The chunks can be written in any order.
    *
    * @param ref the reference of the variable
    * @param chunk a chunk of the variable
    * @param data the values of the variable with correct size (same as the chunk size) and type
    * @throws NetCDFFileWriter when an error occur
    */
  @throws[NetCDFException]
  def writeVariable(ref: VariableRef, chunk: DataChunk, data: DataStorage)
}
