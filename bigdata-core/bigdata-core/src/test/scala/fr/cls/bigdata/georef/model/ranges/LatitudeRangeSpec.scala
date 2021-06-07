package fr.cls.bigdata.georef.model.ranges

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class LatitudeRangeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  import fr.cls.bigdata.georef.model.ranges.LatitudeRange._

  it("should create inclusive ranges of latitude") {
    val ranges = Table(
      ("lower", "higher", "range"),
      (-90D, 90D, Inclusive(-90D, 90D)),
      (-20D, 30D, Inclusive(-20D, 30D)),
      (20D, 20D, Inclusive(20D, 20D)),
      (-10D, -30D, Empty)
    )

    forAll(ranges) { (lower, higher, range) =>
      inclusive(lower, higher) shouldBe range
    }
  }

  it("should throw exceptions when latitude are invalid") {
    val ranges = Table(
      ("lower", "higher"),
      (-91D, 90D),
      (-20D, 95D)
    )

    forAll(ranges) { (lower, higher) =>
      an [IllegalArgumentException] shouldBe thrownBy (inclusive(lower, higher))
    }
  }

  it ("should contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(-46D, 82D), Seq(-46D, -12D, 0D, 36D, 82D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe true)
    }
  }

  it ("should not contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(-46D, 82D), Seq(-90D, -47D, 84D)),
      (Empty, Seq(-90D, -46D, -12D, 0D, 36D, 82D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe false)
    }
  }

  it ("should align to grid") {
    val cases = Table(
      ("range", "origin", "resolution", "result"),
      (Inclusive(-90D, 90D), 0D, 10D, Inclusive(-90D, 90D)),
      (Inclusive(-90D, 89.999D), 0D, 10D, Inclusive(-90D, 80D)),
      (Inclusive(-42D, 75.0001D), -5D, 20D, Inclusive(-25D, 75D)),
      (Inclusive(-19D, 15D), 0D, 20D, Inclusive(0D, 0D)),
      (Inclusive(-15D, -5D), 0D, 20D, Empty),
      (Empty, 0D, 10D, Empty)
    )

    forAll(cases) { (range, origin, resolution, result) =>
      range.alignWithGridInner(origin, resolution) shouldBe result
    }
  }
}
