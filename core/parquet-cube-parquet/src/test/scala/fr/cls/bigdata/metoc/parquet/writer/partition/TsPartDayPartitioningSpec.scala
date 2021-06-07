package fr.cls.bigdata.metoc.parquet.writer.partition

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class TsPartDayPartitioningSpec extends FunSpec with TableDrivenPropertyChecks with Matchers with TestData {
  describe("partitioner") {
    describe("partitions") {
      it(s"should return tspartday partitions") {
        val grids = Table(
          ("grid", "tspartdays"),
          (Day1.grid, Set(Day1.tsPartDay)),
          (Day2.grid, Set(Day2.tsPartDay)),
          (TwoDays.grid, TwoDays.tsPartDays)
        )
        forAll(grids) { (grid, tsPartDays) =>
          val partitions = TsPartDayPartitioning.partitioner(grid).partitions
          partitions.size shouldBe tsPartDays.size
          partitions should contain theSameElementsAs tsPartDays
        }
      }
    }

    describe("partition") {
      val grid = TwoDays.grid
      val coordinates = Table(
        ("coordinates", "tspartDays"),
        (Day1.someCoordinates, Day1.tsPartDay),
        (Day2.someCoordinates, Day2.tsPartDay)
      )
      forAll(coordinates) { (someCoordinates, tsPartDay) =>
        val partition = TsPartDayPartitioning.partitioner(grid).partition(someCoordinates)
        partition shouldBe tsPartDay
      }
    }
  }
}
