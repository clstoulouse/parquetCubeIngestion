package fr.cls.bigdata.netcdf.conversion

import fr.cls.bigdata.georef.model.DataType
import fr.cls.bigdata.georef.metadata.{MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.netcdf.BaseUnitSpec

class VariableConverterSpec extends BaseUnitSpec {
  describe("apply") {
    it("should create  a variable converter") {
      val attributes = Set[MetadataAttribute](
        MetadataAttribute("_FillValue", DataType.Double, Seq(-2147483647D)),
        MetadataAttribute("scale_factor", DataType.Double, Seq(1E-4D))
      )
      val metadata = VariableMetadata("var1", DataType.Double, Seq(), attributes)

      val converter = VariableConverter(metadata)
      converter shouldBe VariableConverter(1E-4D, 0D, -2147483647D, DataType.Double)
    }
  }

  describe("fromNetCDF") {
    it("should return None when fillValue") {
      val variables = Table(
        ("dataType", "fillValue"),
        (DataType.Double, Double.MinValue),
        (DataType.Float, 0.12345F),
        (DataType.Long, 0),
        (DataType.Int, Int.MaxValue),
        (DataType.Short, 42),
        (DataType.Byte, 1)
      )
      forAll(variables) { (dataType, fillValue) =>
        val converter = VariableConverter(None, None, Some(fillValue), dataType)
        converter.fromNetCDF(fillValue) shouldBe None
      }
    }

    it("should return scaled value") {
      val variables = Table(
        ("scale", "value", "result"),
        (0.3D, 1D, 0.3D),
        (20D, 2D, 40D),
        (-1D, 3D, -3D)
      )
      forAll(variables) { (scale, value, result) =>
        val converter = VariableConverter(Some(scale), None, None, DataType.Double)
        converter.fromNetCDF(value) shouldBe Some(result)
      }
    }

    it("should return translated value") {
      val variables = Table(
        ("offset", "value", "result"),
        (0.1D, 1D, 1.1D),
        (12D, 2D, 14D),
        (-5D, 3D, -2D)
      )
      forAll(variables) { (offset, value, result) =>
        val converter = VariableConverter(None, Some(offset), None, DataType.Double)
        converter.fromNetCDF(value) shouldBe Some(result)
      }
    }
  }
}
