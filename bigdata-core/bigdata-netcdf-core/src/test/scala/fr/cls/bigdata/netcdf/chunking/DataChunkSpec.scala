package fr.cls.bigdata.netcdf.chunking

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class DataChunkSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  describe("range") {
    it("should iterate over all values in order") {
      val table = Table(
        ("chunk", "range"),
        (DataChunk(Seq(DimensionChunk(0, 10), DimensionChunk(0, 8), DimensionChunk(0, 2))), Range(0, 160)),
        (DataChunk(Seq(DimensionChunk(10, 20), DimensionChunk(8, 12))), Range(0, 40))
      )

      forAll(table) { (chunk, range) => chunk.range should contain theSameElementsInOrderAs range}
    }
  }

  describe("coordinates") {
    it("should return all the chunk coordinates") {
      val chunk = DataChunk(Seq(DimensionChunk(0, 2), DimensionChunk(5, 8)))
      chunk.coordinates.toSeq should contain theSameElementsInOrderAs Seq(Seq(0, 5), Seq(0, 6), Seq(0, 7), Seq(1, 5), Seq(1, 6), Seq(1, 7))
    }
  }
}
