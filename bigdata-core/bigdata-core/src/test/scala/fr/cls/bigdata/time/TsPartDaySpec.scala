package fr.cls.bigdata.time

import java.time.DateTimeException

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks

class TsPartDaySpec extends FunSpec with Matchers with TableDrivenPropertyChecks {
  describe("apply") {
    it(s"should throw ${classOf[DateTimeException].getSimpleName} when values are invalid") {
      val invalidTsPartDays = Table(
        ("year", "day"),
        (1970, 400),
        (1995, 366),
        (1996, 367),
        (2000, 367)
      )
      forAll(invalidTsPartDays) { (year, day) => a [DateTimeException] should be thrownBy TsPartDay(year, day) }
    }
  }

  describe("stamp"){
    it("should return stamp") {
      val tsPartDays = Table(
        ("tsPartDay", "stamp"),
        (TsPartDay(1970, 1), 1970001),
        (TsPartDay(2001, 42), 2001042),
        (TsPartDay(2019, 128), 2019128)
      )

      forAll(tsPartDays) {(tsPartDay, stamp) => tsPartDay.stamp shouldBe stamp }
    }
  }

  describe("epochMilli") {
    it("should return epoch in milliseconds") {
      val tsPartDays = Table(
        ("tsPartDay", "epoch"),
        (TsPartDay(1970, 1), 0L),
        (TsPartDay(1970, 2), 86400000L),
        (TsPartDay(2001, 42), 981849600000L),
        (TsPartDay(1956, 264), -419126400000L)
      )

      forAll(tsPartDays) {(tsPartDay, epochMilli) => tsPartDay.epochMilli shouldBe epochMilli }
    }
  }

  describe("until") {
    it("should return a range of valid tspartday") {
      val ranges = Table(
        ("from", "end", "range"),
        (TsPartDay(1970, 1), TsPartDay(1970, 4), Seq(TsPartDay(1970, 1), TsPartDay(1970, 2), TsPartDay(1970, 3))),
        (TsPartDay(2015, 46), TsPartDay(2015, 47), Seq(TsPartDay(2015, 46))),
        (TsPartDay(2005, 247), TsPartDay(2005, 247), Seq()),
        // 2004 is a leap year
        (TsPartDay(2004, 366), TsPartDay(2005, 2), Seq(TsPartDay(2004, 366), TsPartDay(2005, 1))),
        // 1900 is not a leap year
        (TsPartDay(1900, 365), TsPartDay(1901, 2), Seq(TsPartDay(1900, 365), TsPartDay(1901, 1))),
        // but 2000 is a leap year
        (TsPartDay(2000, 365), TsPartDay(2001, 2), Seq(TsPartDay(2000, 365), TsPartDay(2000, 366), TsPartDay(2001, 1)))
      )

      forAll(ranges) {(from, end, range) => from.until(end) should contain theSameElementsInOrderAs range}
    }
  }

  describe("to") {
    it("should return a range of valid tspartday") {
      val ranges = Table(
        ("from", "end", "range"),
        (TsPartDay(1970, 1), TsPartDay(1970, 4), Seq(TsPartDay(1970, 1), TsPartDay(1970, 2), TsPartDay(1970, 3), TsPartDay(1970, 4))),
        (TsPartDay(2015, 46), TsPartDay(2015, 47), Seq(TsPartDay(2015, 46), TsPartDay(2015, 47))),
        (TsPartDay(2005, 247), TsPartDay(2005, 247), Seq(TsPartDay(2005, 247))),
        // 2004 is a leap year
        (TsPartDay(2004, 366), TsPartDay(2005, 1), Seq(TsPartDay(2004, 366), TsPartDay(2005, 1))),
        // 1900 is not a leap year
        (TsPartDay(1900, 365), TsPartDay(1901, 1), Seq(TsPartDay(1900, 365), TsPartDay(1901, 1))),
        // but 2000 is a leap year
        (TsPartDay(2000, 365), TsPartDay(2001, 1), Seq(TsPartDay(2000, 365), TsPartDay(2000, 366), TsPartDay(2001, 1)))
      )

      forAll(ranges) {(from, end, range) => from.to(end) should contain theSameElementsInOrderAs range}
    }
  }

}
