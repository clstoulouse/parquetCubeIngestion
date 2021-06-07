package fr.cls.bigdata.netcdf.conversion

import fr.cls.bigdata.georef.model.DataType.NumericType
import fr.cls.bigdata.georef.metadata.VariableMetadata
import fr.cls.bigdata.georef.model.DataType
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.georef.utils.Rounding

case class VariableConverter(scale: Double, offset: Double, fillValue: Double, dataType: NumericType) {

  val rounding = new Rounding(0, Rounding.RoundHalfUp);

  def fromNetCDF(value: Any): Option[Double] = {
    val doubleValue = dataType.toDouble(value)
    if(doubleValue != fillValue) Some(doubleValue * scale + offset) else None
  }

  def toNetCDF(valueOpt: Option[Any]): Any = {
    dataType match {
      case DataType.Byte => dataType.of(valueOpt.map(v => rounding.round((v.asInstanceOf[Double] - offset) / scale)).getOrElse(fillValue))
      case DataType.Int => dataType.of(valueOpt.map(v => rounding.round((v.asInstanceOf[Double] - offset) / scale)).getOrElse(fillValue))
      case _ =>  dataType.of(valueOpt.map(v => (v.asInstanceOf[Double] - offset) / scale).getOrElse(fillValue))
    }
  }
}

object VariableConverter {
  import fr.cls.bigdata.georef.metadata.Constants._

  def apply(metadata: VariableMetadata): VariableConverter = {
    metadata.dataType match {
      case numeric: NumericType =>
        VariableConverter(
          metadata.find(ScaleAttribute).map(_.values.head),
          metadata.find(OffsetAttribute).map(_.values.head),
          metadata.find(FillValueAttribute).map(_.values.head),
          numeric
      )
      case unsupportedType => throw new NetCDFException(s"NetCDF $unsupportedType is not yet supported")
    }
  }

  def apply(scale: Option[Any], offset: Option[Any], fillValue: Option[Any], dataType: NumericType): VariableConverter = {
    VariableConverter(
      scale.map(dataType.toDouble).getOrElse(1D),
      offset.map(dataType.toDouble).getOrElse(0D),
      fillValue.map(dataType.toDouble).getOrElse(Double.NaN),
      dataType
    )
  }
}
