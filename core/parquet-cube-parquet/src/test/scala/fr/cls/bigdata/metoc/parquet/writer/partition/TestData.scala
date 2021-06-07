package fr.cls.bigdata.metoc.parquet.writer.partition

import fr.cls.bigdata.metoc.model.{Coordinates, Grid}

import scala.collection.SortedSet

trait TestData {
  object Day1 {
    val grid = Grid(SortedSet(150D), SortedSet(90D, -12D), SortedSet(1550102384000L), SortedSet())
    val someCoordinates = Coordinates(grid.longitude.head, grid.latitude.head, grid.time.head, None)
    val tsPartDay: Partition = Partition("tspartday=2019044")
  }

  object Day2 {
    val grid = Grid(SortedSet(150D, -59D), SortedSet(-12D), SortedSet(1550102401000L), SortedSet())
    val someCoordinates = Coordinates(grid.longitude.head, grid.latitude.head, grid.time.head, None)
    val tsPartDay: Partition = Partition("tspartday=2019045")
  }

  object TwoDays {
    val grid = Grid(SortedSet(150D, -59D), SortedSet(90D, -12D), SortedSet(1550102384000L, 1550102401000L), SortedSet())
    val tsPartDays: Set[Partition] = Set(Day1.tsPartDay, Day2.tsPartDay)
  }
}
