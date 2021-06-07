package fr.cls.bigdata.netcdf.conversion

import fr.cls.bigdata.georef.model.DataType.NumericType
import fr.cls.bigdata.georef.model.DimensionRef
import fr.cls.bigdata.georef.metadata.DimensionMetadata
import fr.cls.bigdata.netcdf.exception.NetCDFException
import org.joda.time.{DateTime, DateTimeZone, Duration}
import ucar.nc2.units.DateUnit

case class TimeConverter(dataType: NumericType, dateOrigin: DateTime, unit: TimeUnit) extends DimensionConverter {

  final val hoursToMillis = 3600000;
  final val daysToMillis = 86400000;
  final val secondsToMillis = 1000;

/**
  override def fromNetCDF(value: Any): Any = {
    val doubleValue = dataType.toDouble(value)

    val intValue = {
      if (doubleValue <= Int.MaxValue && doubleValue >= Int.MinValue && doubleValue % 1 == 0d) doubleValue.toInt
      else throw new NetCDFException(s"cannot convert $value to Int")
    }

    unit match {
      case TimeUnit.Hours => dateOrigin.plusHours(intValue).getMillis
      case TimeUnit.Days => dateOrigin.plusDays(intValue).getMillis
      case TimeUnit.Seconds => dateOrigin.plusSeconds(intValue).getMillis
    }
  }

  **/


  override def fromNetCDF(value: Any): Any = {

    val doubleValue = dataType.toDouble(value)

    val millisValue = {
      unit match {
        case TimeUnit.Hours => doubleValue*hoursToMillis;
        case TimeUnit.Days => doubleValue*daysToMillis;
        case TimeUnit.Seconds => doubleValue*secondsToMillis;
      }
    }

    val dateOriginMillis = dateOrigin.getMillis

    dateOriginMillis + millisValue.toLong

  }

  override def toNetCDF(value: Any): Any = {
    if (!value.isInstanceOf[Long]) throw new NetCDFException(s"a value of type Long is expected, found $value")
    val date = new DateTime(DateTimeZone.UTC).withMillis(value.asInstanceOf[Long])
    val duration = new Duration(dateOrigin, date)
    unit match {
      case TimeUnit.Hours => dataType.of(duration.getStandardHours)
      case TimeUnit.Days => dataType.of(duration.getStandardDays)
      case TimeUnit.Seconds => dataType.of(duration.getStandardSeconds)
    }
  }
}

object TimeConverter {
  def apply(ref: DimensionRef, metadata: DimensionMetadata, dataType: NumericType): TimeConverter = {
    import fr.cls.bigdata.georef.metadata.Constants._
    val referenceUnit = metadata.findSingleValue[String](UnitsAttribute).map(new DateUnit(_))
      .getOrElse(throw new NetCDFException(s"The unit attribute of $ref is malformed"))

    val timeUnit = referenceUnit.getTimeUnitString match {
      case DaysTimeUnit => TimeUnit.Days
      case HoursTimeUnit => TimeUnit.Hours
      case SecondsTimeUnit => TimeUnit.Seconds
      case unit => throw new NetCDFException(s"NetCDF time unit '$unit' is not supported")
    }

    val dateOrigin = new DateTime(referenceUnit.getDateOrigin, DateTimeZone.UTC)
    TimeConverter(dataType, dateOrigin, timeUnit)
  }
}