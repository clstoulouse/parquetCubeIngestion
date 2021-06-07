package fr.cls.bigdata.netcdf.chunking

import fr.cls.bigdata.georef.model.DimensionRef
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class DataShapeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  describe("chunks") {
    it("should return all chunks in order") {
      val dataShape = DataShape(Seq(DimensionShape(DimensionRef("lon"), 8, 3), DimensionShape(DimensionRef("lat"), 25, 10)))
      dataShape.chunks should contain theSameElementsInOrderAs Seq(
        DataChunk(Seq(DimensionChunk(0, 3), DimensionChunk(0, 10))),
        DataChunk(Seq(DimensionChunk(0, 3), DimensionChunk(10, 20))),
        DataChunk(Seq(DimensionChunk(0, 3), DimensionChunk(20, 25))),
        DataChunk(Seq(DimensionChunk(3, 6), DimensionChunk(0, 10))),
        DataChunk(Seq(DimensionChunk(3, 6), DimensionChunk(10, 20))),
        DataChunk(Seq(DimensionChunk(3, 6), DimensionChunk(20, 25))),
        DataChunk(Seq(DimensionChunk(6, 8), DimensionChunk(0, 10))),
        DataChunk(Seq(DimensionChunk(6, 8), DimensionChunk(10, 20))),
        DataChunk(Seq(DimensionChunk(6, 8), DimensionChunk(20, 25)))
      )
    }

    it("should return chunk of id") {
      val dataShape = DataShape(Seq(DimensionShape(DimensionRef("lon"), 8, 3), DimensionShape(DimensionRef("lat"), 25, 10)))
      dataShape.chunk(6) shouldBe DataChunk(Seq(DimensionChunk(6, 8), DimensionChunk(0, 10)))
    }
  }
}
