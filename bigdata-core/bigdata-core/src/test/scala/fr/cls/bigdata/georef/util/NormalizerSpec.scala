package fr.cls.bigdata.georef.util

import fr.cls.bigdata.georef.utils.{Normalizer, Rounding}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}


class NormalizerSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  private val rounding = Rounding(2, Rounding.RoundHalfUp)

  describe("Longitude") {
    describe("normalize") {
      it("should normalize values in [-180; 180[") {
        val normalizer = Normalizer.Longitude(rounding)
        val values = Table(("value", "result"),
          (-360D, 0D), (-312.2D, 47.8D), (-180D, -180D), (0D, 0D), (75.1D, 75.1D), (180D, -180D), (284.47D, -75.53D), (360D, 0D)
        )
        forAll(values)((value, expectation) => normalizer.normalize(value) shouldBe expectation)
      }

      it("should return rounded values") {
        val normalizer = Normalizer.Longitude(rounding)
        val values = Table(("value", "result"),
          (-179.987D, -179.99D), (-90.456D, -90.46D), (0.001D, 0D), (90.999D, 91D), (179.123D, 179.12D)
        )
        forAll(values)((value, expectation) => normalizer.normalize(value) shouldBe expectation)
      }
    }
  }

  describe("Latitude") {
    it(s"should throw ${classOf[IllegalArgumentException].getSimpleName} when values outside [-90; 90]") {
      val normalizer = Normalizer.Latitude(rounding)
      val values = Table("value", -91D, 180D)
      forAll(values)(value => a[IllegalArgumentException] should be thrownBy normalizer.normalize(value))
    }

    it("should return rounded values") {
      val normalizer = Normalizer.Latitude(rounding)
      val values = Table(("value", "result"),
      (-89.456D, -89.46D), (-44.444D, -44.44D), (-0.8901D, -0.89D), (0.001D, 0D), (45.345D, 45.35D), (89.999D, 90D)
      )
      forAll(values)((value, expectation) => normalizer.normalize(value) shouldBe expectation)
    }
  }
}
