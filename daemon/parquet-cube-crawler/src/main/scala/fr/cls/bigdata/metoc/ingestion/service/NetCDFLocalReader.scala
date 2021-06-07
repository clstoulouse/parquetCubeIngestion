package fr.cls.bigdata.metoc.ingestion.service

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata}
import fr.cls.bigdata.georef.model.{DataType, Dimensions, VariableRef}
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.ingestion.internal.{LocalAccess, NormalizedGrid}
import fr.cls.bigdata.metoc.model.DataPoint
import fr.cls.bigdata.metoc.service.{MetocDatasetAccess, MetocReader}
import fr.cls.bigdata.netcdf.model.NetCDFDimension
import fr.cls.bigdata.netcdf.service.NetCDFFileReader
import fr.cls.bigdata.netcdf.ucar.service.UcarFileReader
import fr.cls.bigdata.resource.Resource
import org.apache.commons.io.FilenameUtils

object NetCDFLocalReader extends MetocReader[Iterator[DataPoint]] with LazyLogging {
  /**
    * Read the grid and variables of the NetCDF file to create an instance of [[fr.cls.bigdata.metoc.service.MetocDatasetAccess]].
    * The variables are filtered according to the config.
    * The grid values are rounded according to the precision and rounding method in the config.
    * Throws errors when:
    * - a dimension is unknown
    * - longitude, latitude or time dimension is missing
    * - the type of the dimension cannot be cast to double
    *
    * @param file               A netCDF file.
    * @param rounding           The rounding method to apply to the raw values
    * @param variablesToExclude A set of netCDF variables to exclude
    * @throws MetocReaderException when a error occur while reading the NetCDF file.
    * @return the MetocReader of the NetCDF file.
    */
  def read(file: PathWithFileSystem, rounding: Rounding, variablesToExclude: Set[String]): Resource[MetocDatasetAccess[Iterator[DataPoint]]] = {
    for (netcdfReader <- UcarFileReader(file, variablesToExclude)) yield read(netcdfReader, rounding)
  }

  def read(netCDFFile: NetCDFFileReader, rounding: Rounding): MetocDatasetAccess[Iterator[DataPoint]] = {
    logger.debug(s"creating a METOC reader of the NetCDF file [${netCDFFile.file}]")

    val grid = NormalizedGrid(netCDFFile, rounding)

    logger.debug(s"${Dimensions.longitude} dimension contains ${grid.longitudes.size} coordinates" +
      s" starting with ${grid.longitudes.take(4).mkString(", ")}... ")
    logger.debug(s"${Dimensions.latitude} dimension contains ${grid.latitudes.size} coordinates" +
      s" starting with ${grid.latitudes.take(4).mkString(", ")}... ")
    logger.debug(s"${Dimensions.time} dimension contains ${grid.times.size} coordinates" +
      s" starting with ${grid.times.take(4).mkString(", ")}... ")
    logger.debug(s"${Dimensions.depth} dimension contains ${grid.depths.size} coordinates" +
      s" starting with ${grid.depths.take(4).mkString(", ")}... ")

    val selectedVariables = netCDFFile.variables
      .filter(v => v.shape.is3D || v.shape.is4D)
      //.filterNot(v => variablesToExclude.contains(v.ref))

    logger.debug(s"Found ${selectedVariables.size} variables, after filtering: " + selectedVariables.map(_.ref))

    val name = FilenameUtils.getBaseName(netCDFFile.file.toString)

    val metadata = DatasetMetadata(
      //netCDFFile.dimensions.map(d=> d.ref -> d.metadata).toMap,
      netCDFFile.dimensions.map(
        d =>
          d.ref match {
            case Dimensions.longitude => {

              val inferredDatatype: DataType = d.dataType

              d.ref -> d.metadata
                .replaceAttribute(false,"valid_min", inferredDatatype, inferredDatatype match {
                  case DataType.Float => d.metadata.findSingleValue("valid_min").getOrElse(0f)-180
                  case DataType.Double => d.metadata.findSingleValue("valid_min").getOrElse(0d)-180
                  case DataType.Long => d.metadata.findSingleValue("valid_min").getOrElse(0L)-180
                  case DataType.Int => d.metadata.findSingleValue("valid_min").getOrElse(0)-180
                  case _ => d.metadata.findSingleValue("valid_min").getOrElse(0d)-180
                })
                .replaceAttribute(false,"valid_max", inferredDatatype, inferredDatatype match {
                  case DataType.Float => d.metadata.findSingleValue("valid_max").getOrElse(360f)-180
                  case DataType.Double => d.metadata.findSingleValue("valid_max").getOrElse(360d)-180
                  case DataType.Long => d.metadata.findSingleValue("valid_max").getOrElse(360L)-180
                  case DataType.Int => d.metadata.findSingleValue("valid_max").getOrElse(360)-180
                  case _ => d.metadata.findSingleValue("valid_max").getOrElse(360d)-180
                })
            }
            case _ => d.ref -> d.metadata
          }).toMap,
      selectedVariables.map(v => v.ref -> v.metadata).toMap,
      netCDFFile.globalAttributes
    )

    val dataAccess = new LocalAccess(netCDFFile, grid, selectedVariables)

    MetocDatasetAccess(name, metadata, grid.toMetoc, dataAccess)
  }
}
