package fr.cls.bigdata.metoc.ingestion.internal

import fr.cls.bigdata.georef.metadata.DimensionMetadata
import fr.cls.bigdata.georef.model.{DataType, DimensionRef, Dimensions}
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.ingestion.TestData
import fr.cls.bigdata.netcdf.model.NetCDFDimension
import fr.cls.bigdata.netcdf.service.NetCDFFileReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class NormalizedGridSpec extends FunSpec with Matchers with MockFactory with TableDrivenPropertyChecks with TestData {
  describe("apply") {
    it("should init grid with 4D dimensions") {
      val fileReader = stub[NetCDFFileReader]
      (fileReader.dimensions _).when().returns(Mixed3DAnd4D.netCDFDimensions)
      (fileReader.variables _).when().returns(Mixed3DAnd4D.variables)
      (fileReader.globalAttributes _).when().returns(globalAttributes)
      (fileReader.read(_: DimensionRef)).when(Dimensions.longitude).returns(Longitude.storage)
      (fileReader.read(_: DimensionRef)).when(Dimensions.latitude).returns(Latitude.storage)
      (fileReader.read(_: DimensionRef)).when(Dimensions.depth).returns(Depth.storage)
      (fileReader.read(_: DimensionRef)).when(Dimensions.time).returns(Time.storage)

      val grid = NormalizedGrid(fileReader, defaultRounding)

      grid shouldBe Mixed3DAnd4D.grid
    }

    it("should init grid with 3D dimensions") {
      val file = stub[NetCDFFileReader]
      (file.dimensions _).when().returns(Only3D.netCDFDimensions)
      (file.variables _).when().returns(Only3D.variables)
      (file.globalAttributes _).when().returns(globalAttributes)
      (file.read(_: DimensionRef)).when(Dimensions.longitude).returns(Longitude.storage)
      (file.read(_: DimensionRef)).when(Dimensions.latitude).returns(Latitude.storage)
      (file.read(_: DimensionRef)).when(Dimensions.time).returns(Time.storage)

      val grid = NormalizedGrid(file, defaultRounding)

      grid shouldBe Only3D.grid
    }

    it(s"should throw ${classOf[MetocReaderException].getSimpleName} when unknown dimension") {
      val unknownDimension = NetCDFDimension(DimensionRef("unknown"), DimensionMetadata("unknown", DataType.Float, Set()))
      val file = stub[NetCDFFileReader]
      (file.dimensions _).when().returns(Only3D.netCDFDimensions :+ unknownDimension)
      (file.variables _).when().returns(Only3D.variables)
      (file.globalAttributes _).when().returns(globalAttributes)
      (file.read(_: DimensionRef)).when(*).returns(DataType.Double.store(Seq.empty))

      a [MetocReaderException] should be thrownBy NormalizedGrid(file, defaultRounding)
    }

    it(s"should throw ${classOf[MetocReaderException].getSimpleName} when a dimension is missing") {
      val missingDimensions = Table("missing dimension", Dimensions.longitude, Dimensions.latitude, Dimensions.time)

      forAll(missingDimensions) { missingDimension =>
        val file = stub[NetCDFFileReader]

        (file.dimensions _).when().returns(Only3D.netCDFDimensions.filter(_.ref != missingDimension))
        (file.variables _).when().returns(Only3D.variables)
        (file.globalAttributes _).when().returns(globalAttributes)
        (file.read(_: DimensionRef)).when(*).returns(DataType.Double.store(Seq.empty))

        an[MetocReaderException] should be thrownBy NormalizedGrid(file, defaultRounding)
      }
    }
  }
}
