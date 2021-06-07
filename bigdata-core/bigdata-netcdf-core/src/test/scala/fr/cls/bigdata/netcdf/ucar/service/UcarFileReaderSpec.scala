package fr.cls.bigdata.netcdf.ucar.service

import fr.cls.bigdata.georef.metadata.MetadataAttribute
import fr.cls.bigdata.georef.model.{DataType, VariableRef}
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DimensionChunk}
import fr.cls.bigdata.netcdf.{BaseUnitSpec, TestData}
import ucar.nc2.NetcdfFile

class UcarFileReaderSpec extends BaseUnitSpec with TestData with HadoopTestUtils {
  private val inputFile = toHadoopPath(getClass.getResource("/netcdf/nrt_global_allsat_phy_l4_20180930_20181006.nc").getPath)
  private val ucarFile = NetcdfFile.open(inputFile.toString())
  private val file = UcarFileReader(inputFile, ucarFile)
  private val variableRef = VariableRef("sea_surface_height_above_geoid")
  private val chunk = DataChunk(Seq(
    DimensionChunk(0, 1),
    DimensionChunk(0, 720),
    DimensionChunk(0, 1440)
  ))

  describe("globalAttributes") {
    it("should read global attributes") {
      val attributes = file.globalAttributes

      attributes should have size 44

      attributes.find(_.name == "geospatial_lat_max").get.dataType shouldBe DataType.Double
      attributes.find(_.name == "geospatial_lat_max").get.values should contain theSameElementsInOrderAs Seq(89.875)
      attributes.find(_.name == "geospatial_vertical_units").get.dataType shouldBe DataType.String
      attributes.find(_.name == "geospatial_vertical_units").get.values should contain theSameElementsInOrderAs Seq("m")
    }
  }

  describe("dimensions") {
    it("should read dimension metadata") {
      file.dimensions should have size 3


      val longitude = file.dimensions.find(_.ref == Longitude.ref).get
      val latitude = file.dimensions.find(_.ref == Latitude.ref).get
      val time = file.dimensions.find(_.ref == Time.ref).get

      latitude.dataType shouldBe DataType.Float
      latitude.metadata.attributes should have size 7
      latitude.metadata.attributes should contain allElementsOf Seq(
        MetadataAttribute("axis", DataType.String, Seq("Y")),
        MetadataAttribute("valid_max", DataType.Double, Seq(89.875))
      )

      longitude.dataType shouldBe DataType.Float
      longitude.metadata.attributes should have size 7
      longitude.metadata.attributes should contain allElementsOf Seq(
        MetadataAttribute("axis", DataType.String, Seq("X")),
        MetadataAttribute("valid_max", DataType.Double, Seq(359.875))
      )

      time.dataType shouldBe DataType.Float
      time.metadata.attributes should have size 5
      time.metadata.attributes should contain allElementsOf Seq(
        MetadataAttribute("axis", DataType.String, Seq("T")),
        MetadataAttribute("calendar", DataType.String, Seq("gregorian")),
        MetadataAttribute("long_name", DataType.String, Seq("Time")),
        MetadataAttribute("standard_name", DataType.String, Seq("time")),
        MetadataAttribute("units", DataType.String, Seq("days since 1950-01-01 00:00:00"))
      )
    }
  }

  describe("variables") {
    it("should parse variables") {
      file.variables should have size 8

      val variable = file.variables.find(_.ref == variableRef).get

      variable.metadata.dataType shouldBe DataType.Int
      variable.shape.dimensions.map(_.ref) should contain theSameElementsAs Seq(Longitude.ref, Latitude.ref, Time.ref)

      val attributes = variable.metadata.attributes
      attributes should have size 8
      attributes.find(_.name == "_FillValue").get shouldBe MetadataAttribute("_FillValue", DataType.Int, Seq(-2147483647))
      attributes.map(_.name) should contain allElementsOf Seq("comment", "comment", "grid_mapping", "long_name", "scale_factor", "standard_name", "units")
    }
  }

  describe("readDimension") {
    it("should return all values forall dimension") {
      file.read(Longitude.ref).size shouldBe 1440
      file.read(Latitude.ref).size shouldBe 720
      file.read(Time.ref).size shouldBe 1
    }
  }

  describe("read") {
    it("should return all values") {
      file.read(variableRef, chunk) should have size 720*1440
    }

    it ("should return correct values") {
      val allValues = file.read(variableRef, chunk)
      val dataPoints = Table(
        ("index", "value"),
        (0, -2147483647),
        (1035360, -2147483647),
        (1439, -2147483647),
        (1036799, -2147483647),
        (817714, -2116)
      )
      forAll(dataPoints) { (index, value) => allValues(index) shouldBe value }
    }

  }
}
