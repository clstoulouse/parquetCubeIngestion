package fr.cls.bigdata.netcdf.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.georef.model.DimensionRef
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs.Path

trait NetCDFFileBuilder {
  /**
    * Create a netCDF file that is initialized with the given dataset metadata and dimension lengths.
    * Returns a [[fr.cls.bigdata.netcdf.service.NetCDFFileWriter]] in which it is possible to write the dimension and variable values
    *
    * The [[fr.cls.bigdata.netcdf.service.NetCDFFileWriter]] resource must be closed after usage.
    *
    * @param metadata the netCDF metadata that describe the type and attributes of the dimensions and variables
    * @param dimensionLengths the length of each dimension
    * @param path the path to create the netCDF file to
    * @throws NetCDFException when an error occur
    * @return a netCDF writer resource
    */
  @throws[NetCDFException]
  def create(metadata: DatasetMetadata, dimensionLengths: Map[DimensionRef, Int], path: Path): Resource[NetCDFFileWriter]
}
