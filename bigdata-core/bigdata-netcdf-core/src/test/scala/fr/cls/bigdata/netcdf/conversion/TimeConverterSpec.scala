package fr.cls.bigdata.netcdf.conversion

import fr.cls.bigdata.georef.model.DataType
import fr.cls.bigdata.netcdf.BaseUnitSpec
import fr.cls.bigdata.netcdf.exception.NetCDFException
import org.joda.time.{DateTime, DateTimeZone}

class TimeConverterSpec extends BaseUnitSpec {
  describe("fromNetCDF") {
    val epoch = new DateTime(DateTimeZone.UTC).withMillis(0L)

  /**
    it(s"should throw ${classOf[NetCDFException].getSimpleName} when values are not integer") {
      val converter = TimeConverter(DataType.Double, epoch, TimeUnit.Days)
      val values = Table("value", Long.MaxValue, Long.MinValue, 0.1F, -0.1D)
      forAll(values)(value => a[NetCDFException] should be thrownBy converter.fromNetCDF(value))
    }
   **/

    it("should return timestamp") {
      val someDay = new DateTime(DateTimeZone.UTC).withMillis(1549843200000L)
      val netCDFTimes = Table(
        ("origin", "unit", "value", "result"),
        (epoch, TimeUnit.Days, 0, 0L), (epoch, TimeUnit.Hours, 1, 3600000L), (epoch, TimeUnit.Seconds, 2, 2000L),
        (someDay, TimeUnit.Days, 2, 1550016000000L),
        (someDay, TimeUnit.Hours, 1, 1549846800000L),
        (someDay, TimeUnit.Seconds, 100, 1549843300000L)
      )
      forAll(netCDFTimes) { (origin, unit, value, result ) =>
        val converter = TimeConverter(DataType.Int, origin, unit)
        converter.fromNetCDF(value) shouldBe result
      }
    }
  }
}
