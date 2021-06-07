package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.netcdf.{BaseUnitSpec, TestData}

import scala.collection.convert.DecorateAsJava

private[ucar] class UcarArrayBuilderSpec extends BaseUnitSpec with TestData with DecorateAsJava {
  describe("buildArray") {
    it("should build the array of dimension values") {
      val dimensions = Table(
        ("raw data", "length"),
        (Longitude.rawData, Longitude.length),
        (Latitude.rawData, Latitude.length),
        (Time.rawData, Time.length),
        (Depth.rawData, Depth.length)
      )

      forAll(dimensions) { (rawData, length) =>
        val builder = createArrayBuilder
        val array = builder.buildArray(rawData, Array(length))
        for((value, index) <- rawData.zipWithIndex) array.getObject(index) shouldBe value
      }
    }

    it("should build the array of variable values") {
      val variables = Table(
        ("shape", "rawData"),
        (Variable3D.shape, Variable3D.rawData),
        (Variable4D.shape, Variable4D.rawData)
      )
      forAll(variables) {(shape, rawData) =>
        val dimensions = shape.dimensions.map(_.totalSize).toArray
        val builder = createArrayBuilder
        val array = builder.buildArray(rawData, dimensions)
        for((value, index) <- rawData.zipWithIndex) array.getObject(index) shouldBe value
      }
    }
  }

  def createArrayBuilder: UcarArrayBuilder = new UcarArrayBuilder {}
}
