package fr.cls.bigdata.georef.model

import fr.cls.bigdata.georef.model.ranges.{LatitudeRange, LongitudeRange, TimeRange}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class RegularGridSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  describe("apply") {
    it("should compute the bounds of the density map") {
      val resolution = Resolution(10, 20, 100)
      val origin = Position.`3D`(0D, 0D, 0L)

      val testCases = Table(
        ("bounds", "expected"),
        (Bounds(-180D, 180D, -90D, 90D, 0L, 1000L), Bounds(-180D, 180D, -80D, 80D, 0L, 1000L)),
        (Bounds(-179.999D, 180D, -90D, 89.999D, 1L, 999L), Bounds(-170D, 180D, -80D, 80D, 100L, 900L)),
        (Bounds(-5.5D, 15.3D, -42D, 80.001D, 99L, 501L), Bounds(0D, 10D, -40D, 80D, 100L, 500L)),
        (Bounds(-5.5D, 9D, -19D, 15D, 78L, 123L), Bounds(0D, 0D, 0D, 0D, 100L, 100L)),
        (Bounds(0.3D, 9D, -19D, -5D, 103L, 123L), Bounds(LongitudeRange.Empty, LatitudeRange.Empty, TimeRange.Empty))
      )
      forAll (testCases) { (bounds, expected) =>
        RegularGrid(resolution, origin, bounds).bounds shouldBe expected
      }
    }
  }

}
