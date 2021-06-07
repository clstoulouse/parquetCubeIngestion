package fr.cls.bigdata.netcdf.service

import fr.cls.bigdata.georef.metadata.MetadataAttribute
import fr.cls.bigdata.georef.model.{DataStorage, DimensionRef, VariableRef}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.netcdf.chunking.DataChunk
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.netcdf.model.{NetCDFDimension, NetCDFVariable}
import fr.cls.bigdata.netcdf.ucar.service.UcarFileReader
import fr.cls.bigdata.resource.Resource

trait NetCDFFileReader {
  def file: PathWithFileSystem
  def variables: Seq[NetCDFVariable]
  def dimensions: Seq[NetCDFDimension]
  def globalAttributes: Set[MetadataAttribute]

  /**
    * Reads the values of a dimension.
    *
    * @param ref A dimension of the NetCDF file.
    * @return the values of the dimension in order.
    * @throws NetCDFException when a error occur while reading the file or casting the value
    */
  @throws[NetCDFException]
  def read(ref: DimensionRef): DataStorage

  /**
    * Reads all values of the given variable, chunk by chunk, in order
    *
    * @param ref a variable of the NetCDF file
    * @throws NetCDFException when a error occur while reading the file
    * @return the values of the variable, in the order of its chunk
    */
  @throws[NetCDFException]
  def read(ref: VariableRef, chunk: DataChunk): DataStorage
}

object NetCDFFileReader {
  def apply(file: PathWithFileSystem): Resource[NetCDFFileReader] = UcarFileReader(file)
}
