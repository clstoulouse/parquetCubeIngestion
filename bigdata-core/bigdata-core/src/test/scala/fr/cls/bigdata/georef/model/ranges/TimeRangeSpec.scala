package fr.cls.bigdata.georef.model.ranges

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class TimeRangeSpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  import fr.cls.bigdata.georef.model.ranges.TimeRange._

  it("should create inclusive ranges of time") {
    val ranges = Table(
      ("lower", "higher", "range"),
      (0L, 10000L, Inclusive(0L, 10000L)),
      (-10000L, 10000L, Inclusive(-10000L, 10000L)),
      (10000L, -1000L, Empty)
    )

    forAll(ranges) { (lower, higher, range) =>
      inclusive(lower, higher) shouldBe range
    }
  }

  it ("should contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(10000L, 20000L), Seq(10000L, 11000L, 20000L))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe true)
    }
  }

  it ("should not contains value") {
    val cases = Table(
      ("range", "values"),
      (Inclusive(10000L, 20000L), Seq(-10000L, 0L, 21000L)),
      (Empty, Seq(-10000L, 0L, 10000L, 11000L, 20000L, 21000L))
    )

    forAll(cases){ (range, values) =>
      values.foreach(range.contains(_) shouldBe false)
    }
  }

  it("should shit") {
    val cases = Table(
      ("range", "shift", "result"),
      (Inclusive(10000L, 20000L), 5000L, Inclusive(5000L, 15000L)),
      (Empty, 5000L, Empty)
    )

    forAll(cases) { (range, shift, result) =>
      range.minus(shift) shouldBe result
    }
  }

  it ("should align to grid") {
    val cases = Table(
      ("range", "origin", "resolution", "result"),
      (Inclusive(10000L, 20000L), 0L, 1000L, Inclusive(10000L, 20000L)),
      (Inclusive(10000L, 20000L), 5000L, 10000L, Inclusive(15000L, 15000L)),
      (Inclusive(10000L, 20000L), 0L, 40000L, Empty),
      (Empty, 0L, 1000L, Empty)
    )

    forAll(cases) { (range, origin, resolution, result) =>
      range.alignWithGridInner(origin, resolution) shouldBe result
    }
  }
}
