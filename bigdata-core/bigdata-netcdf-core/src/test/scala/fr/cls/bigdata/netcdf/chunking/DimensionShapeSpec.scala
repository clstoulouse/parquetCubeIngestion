package fr.cls.bigdata.netcdf.chunking

import fr.cls.bigdata.georef.model.DimensionRef
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class DimensionShapeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  val mockRef = DimensionRef("lon")

  describe("chunks") {
    it ("should return the right number of chunks") {
      val table = Table(
        ("shape", "number of chunks"),
        (DimensionShape(mockRef, 10, 3), 4),
        (DimensionShape(mockRef, 128, 8), 16),
        (DimensionShape(mockRef, 123, 123), 1)
      )

      forAll(table) { (shape, chunkCount) => shape.chunkCount shouldBe chunkCount}
    }

    it ("should return the chunks in order") {
      val shape = DimensionShape(mockRef, 10, 3)
      shape.chunks should contain theSameElementsInOrderAs Seq(
        DimensionChunk(0, 3),
        DimensionChunk(3, 6),
        DimensionChunk(6, 9),
        DimensionChunk(9, 10))
    }
  }

}
