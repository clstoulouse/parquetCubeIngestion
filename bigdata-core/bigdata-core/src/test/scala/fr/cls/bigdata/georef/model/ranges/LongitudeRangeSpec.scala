package fr.cls.bigdata.georef.model.ranges

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks

class LongitudeRangeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  import LongitudeRange._

  it("should create inclusive ranges of longitude") {
    val ranges = Table(
      ("lower", "higher", "range"),
      (-180D, 180D, Inclusive(-180D, 180D)),
      (15D, 30D, Inclusive(15D, 30D)),
      (-430D, -340D, Inclusive(-70D, 20D)),
      (-10D, -10D, Inclusive(-10D, -10D)),
      (240D, 300D, Inclusive(-120D, -60D)),
      (179D, 181D, AntiMeridianInclusive(179D, 181D)),
      (240D, 580D, AntiMeridianInclusive(-120D, 220D)),
      (-360D, 0D, Inclusive(-180D, 180D)),
      (0D, 360D, Inclusive(-180D, 180D)),
      (160D, 580D, Inclusive(-180D, 180D)),
      (10D, -10D, Empty),
      (170D, -170D, Empty)
    )

    forAll(ranges) { (lower, higher, range) =>
      inclusive(lower, higher) shouldBe range
    }
  }

  it ("should contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(-46D, 82D), Seq(-46D, -12D, 0D, 36D, 82D)),
      (AntiMeridianInclusive(82D, 314D), Seq(82D, 90D, 179D, -180D, -90D, -46D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe true)
    }
  }

  it ("should not contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(-46D, 82D), Seq(-180D, -90D, 90D, 179D)),
      (AntiMeridianInclusive(82D, -314D), Seq(-12D, 0D, 36D)),
      (Empty, Seq(-180D, -90D, -46D, -12D, 0D, 36D, 82D, 90D, 179D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe false)
    }
  }

  it ("should align to grid") {
    val cases = Table(
      ("range", "origin", "resolution", "result"),
      (Inclusive(-180D, 180D), 0D, 10D, Inclusive(-180D, 180D)),
      (Inclusive(-179.999D, 180D), 0D, 10D, Inclusive(-170D, 180D)),
      (Inclusive(-4.5D, 14.7D), 0D, 5D, Inclusive(0D, 10D)),
      (Inclusive(-4.5D, 14.7D), 0.5D, 5D, Inclusive(-4.5D, 10.5D)),
      (Inclusive(-4.5D, 14.7D), 0D, 20D, Inclusive(0D, 0D)),
      (Inclusive(-4.5D, 14.7D), -5D, 20D, Empty),
      (AntiMeridianInclusive(150D, 210D), 0D, 10D, AntiMeridianInclusive(150D, 210D)),
      (AntiMeridianInclusive(150D, 210D), 0D, 20D, AntiMeridianInclusive(160D, 200D)),
      (AntiMeridianInclusive(150D, 210D), 5D, 20D, AntiMeridianInclusive(165D, 205D)),
      (AntiMeridianInclusive(160D, 400D), 0D, 80D, AntiMeridianInclusive(160D, 400D)),
      (AntiMeridianInclusive(175D, 210D), 5D, 20D, Inclusive(-175D, -155D)),
      (AntiMeridianInclusive(170D, 190D), 0D, 40D, Empty),
      (Empty, 0D, 10D, Empty)
    )

    forAll(cases) { (range, origin, resolution, result) =>
      range.alignWithGridInner(origin, resolution) shouldBe result
    }
  }
}
