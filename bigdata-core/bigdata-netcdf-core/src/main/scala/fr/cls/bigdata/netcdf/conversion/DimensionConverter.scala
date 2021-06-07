package fr.cls.bigdata.netcdf.conversion

import fr.cls.bigdata.georef.model.DataType.NumericType
import fr.cls.bigdata.georef.metadata.DimensionMetadata
import fr.cls.bigdata.georef.model.{DimensionRef, Dimensions}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.netcdf.model.NetCDFDimension

trait DimensionConverter {
  @throws[NetCDFException]
  def fromNetCDF(value: Any): Any

  @throws[NetCDFException]
  def toNetCDF(value: Any): Any
}

object DimensionConverter {
  def apply(dimension: NetCDFDimension): DimensionConverter = {
    DimensionConverter(dimension.ref, dimension.metadata)
  }


  def apply(ref: DimensionRef, metadata: DimensionMetadata): DimensionConverter = {
    metadata.dataType match {
      case numeric: NumericType =>
        ref match {
          case Dimensions.time => TimeConverter(ref, metadata, numeric)
          case _ => DefaultConverter(numeric)
        }
      case notNumeric => throw new NetCDFException(s"Type $notNumeric of $ref is not yet supported")
    }
  }

  case class DefaultConverter(numeric: NumericType) extends DimensionConverter {
    def fromNetCDF(value: Any): Any = numeric.toDouble(value)
    override def toNetCDF(value: Any): Any = numeric.of(value)
  }
}
