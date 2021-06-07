package fr.cls.bigdata.netcdf.ucar.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.netcdf.{BaseUnitSpec, TestData}

import scala.collection.convert.DecorateAsScala

class UcarFileBuilderSpec extends BaseUnitSpec with TestData with DecorateAsScala with HadoopTestUtils {
  import fr.cls.bigdata.netcdf.ucar.internal.UcarMapper._

  describe("addGlobalAttribute") {
    it("should create a netcdf file with a globalAttribute") {
      val folder = HadoopTestUtils.createTempDir()
      val path = folder.child("file.nc4").path

      val metadata = DatasetMetadata(Map(), Map(), Set(someAttribute))

      for (fileWriter <- UcarFileBuilder().create(metadata, Map(), path)) {
        val file = fileWriter.file

        val attributes = file.getGlobalAttributes.asScala
        attributes should have size 1
        attributes.head.getShortName shouldBe someAttribute.name
        attributes.head.getDataType shouldBe toUcarType(someAttribute.dataType)
        attributes.head.getLength shouldBe someAttribute.values.length
      }
    }
  }

  describe("addDimension") {
    it("should create a netcdf file with longitude dimension") {
      val folder = HadoopTestUtils.createTempDir()
      val path = folder.child("file.nc4").path

      val metadata = DatasetMetadata(Map(Longitude.ref -> Longitude.metadata), Map(), Set())
      val dimensionsLength = Map(Longitude.ref -> Longitude.length)

      for (fileWriter <- UcarFileBuilder().create(metadata, dimensionsLength, path)) {
        val file = fileWriter.file

        val dimensions = file.getDimensions.asScala
        dimensions should have size 1
        dimensions.head.getShortName shouldBe Longitude.shortName
        dimensions.head.getLength shouldBe Longitude.length

        val variables = file.getVariables.asScala
        variables should have size 1
        variables.head.getShortName shouldBe Longitude.shortName
        variables.head.getDataType shouldBe toUcarType(Longitude.dataType)

        val attributes = variables.head.getAttributes.asScala
        attributes should have size 1
        attributes.head.getShortName shouldBe someAttribute.name
        attributes.head.getDataType shouldBe toUcarType(someAttribute.dataType)
        attributes.head.getLength shouldBe someAttribute.values.length
      }
    }
  }

  describe("addVariable") {
    it("should create a netcdf file with a 3D variable") {
      val folder = HadoopTestUtils.createTempDir()
      val path = folder.child("file.nc4").path

      val dimensions = Map(Longitude.ref -> Longitude.metadata, Latitude.ref -> Latitude.metadata, Time.ref -> Time.metadata)
      val metadata = DatasetMetadata(dimensions, Map(Variable3D.ref -> Variable3D.metadata), Set())
      val dimensionsLength = Map(Longitude.ref -> Longitude.length, Latitude.ref -> Latitude.length, Time.ref -> Time.length)

      for (fileWriter <- UcarFileBuilder().create(metadata, dimensionsLength, path)) {
        val file = fileWriter.file

        val variable = file.getVariables.asScala.find(v => v.getShortName == Variable3D.shortName).get
        variable.getDataType shouldBe toUcarType(Variable3D.dataType)

        val attributes = variable.getAttributes.asScala
        attributes should have size 2
        attributes.head.getShortName shouldBe someAttribute.name
        attributes.head.getDataType shouldBe toUcarType(someAttribute.dataType)
        attributes.head.getLength shouldBe someAttribute.values.length
      }
    }
  }
}
