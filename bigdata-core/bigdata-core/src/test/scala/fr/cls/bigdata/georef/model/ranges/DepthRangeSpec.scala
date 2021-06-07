package fr.cls.bigdata.georef.model.ranges

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class DepthRangeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  import fr.cls.bigdata.georef.model.ranges.DepthRange._

  it("should create inclusive ranges of depth") {
    val ranges = Table(
      ("lower", "higher", "range"),
      (0D, 10D, Inclusive(0D, 10D)),
      (-10D, 10D, Inclusive(-10D, 10D)),
      (10D, -10D, Empty)
    )

    forAll(ranges) { (lower, higher, range) =>
      inclusive(lower, higher) shouldBe range
    }
  }

  it ("should contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(10D, 20D), Seq(10D, 15D, 20D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe true)
    }
  }

  it ("should not contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(10D, 20D), Seq(-10D, 0D, 22D)),
      (Empty, Seq(-10D, 0D, 10D, 15D, 20D, 22D))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe false)
    }
  }
}
