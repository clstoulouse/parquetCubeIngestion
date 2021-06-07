package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.model.DataType
import fr.cls.bigdata.georef.metadata.MetadataAttribute
import fr.cls.bigdata.netcdf.conversion.{TimeUnit => UcarTimeUnit}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import ucar.ma2.{DataType => UcarDataType}
import ucar.nc2.{Attribute => UcarAttribute}

private[ucar] object UcarMapper {
  import fr.cls.bigdata.georef.metadata.Constants._

  def toTimeUnit(unit: String): UcarTimeUnit = unit match {
    case DaysTimeUnit => UcarTimeUnit.Days
    case HoursTimeUnit => UcarTimeUnit.Hours
    case SecondsTimeUnit => UcarTimeUnit.Seconds
    case _ => throw new NetCDFException(s"Time unit '$unit' is not supported")
  }

  def toMetocType(dataType: UcarDataType): DataType = {
    dataType match {
      case UcarDataType.DOUBLE => DataType.Double
      case UcarDataType.FLOAT => DataType.Float
      case UcarDataType.LONG => DataType.Long
      case UcarDataType.INT => DataType.Int
      case UcarDataType.SHORT => DataType.Short
      case UcarDataType.BYTE => DataType.Byte
      case UcarDataType.STRING => DataType.String
      case UcarDataType.BOOLEAN => DataType.Boolean
      case _ => throw new NetCDFException(s"Cannot convert '$dataType' to NetCDF type")
    }
  }

  def toNumericType(dataType: UcarDataType): DataType.NumericType = {
    toMetocType(dataType) match {
      case numeric: DataType.NumericType => numeric
      case other => throw new NetCDFException(s"Data-type '$other' is not numeric")
    }
  }

  def toUcarType(dataType: DataType): UcarDataType = {
    dataType match {
      case DataType.Byte => UcarDataType.BYTE
      case DataType.Short => UcarDataType.SHORT
      case DataType.Int => UcarDataType.INT
      case DataType.Long => UcarDataType.LONG
      case DataType.Float => UcarDataType.FLOAT
      case DataType.Double => UcarDataType.DOUBLE
      case DataType.Boolean => UcarDataType.BOOLEAN
      case DataType.String => UcarDataType.STRING
    }
  }

  def toUcarNumericType(dataType: DataType): UcarDataType = {
    dataType match {
      case numeric: DataType.NumericType => toUcarType(numeric)
      case other => throw new NetCDFException(s"Data-type '$other' is not numeric")
    }
  }

  def toUcarAttribute(attribute: MetadataAttribute): UcarAttribute = {
    import scala.collection.JavaConverters._
    new UcarAttribute(attribute.name, attribute.values.asJava)
  }
}
