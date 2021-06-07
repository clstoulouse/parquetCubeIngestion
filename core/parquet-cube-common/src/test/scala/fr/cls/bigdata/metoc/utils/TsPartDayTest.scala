package fr.cls.bigdata.metoc.utils

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}

class TsPartDayTest extends FunSpec with Matchers {

  describe("TsPartDayTest.fromTime") {

    it("2019-01-01T00:00:00Z -> 2019001") {
      TsPartDay.fromTime(Instant.parse("2019-01-01T00:00:00Z").toEpochMilli) shouldBe 2019001
    }

    it("2019-05-03T15:22:00Z -> 2019123") {
      TsPartDay.fromTime(Instant.parse("2019-05-03T15:22:00Z").toEpochMilli) shouldBe 2019123
    }

    it("2018-12-31T12:13:00Z -> 2018365") {
      TsPartDay.fromTime(Instant.parse("2018-12-31T12:13:00Z").toEpochMilli) shouldBe 2018365
    }

    it("2018-12-31T00:00:00Z -> 2018365") {
      TsPartDay.fromTime(Instant.parse("2018-12-31T00:00:00Z").toEpochMilli) shouldBe 2018365
    }

  }
}
