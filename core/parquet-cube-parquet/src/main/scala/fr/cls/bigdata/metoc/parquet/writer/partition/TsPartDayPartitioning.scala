package fr.cls.bigdata.metoc.parquet.writer.partition

import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.metoc.utils.TsPartDay

/**
  * Partitioning strategy that consists of partitioning parquet files by day (relying exclusively on the value of the `time` dimension).<br />
  * Each day will correspond to a folder called `tspartday=YYYYDDD` where `YYYYDDD` part corresponds to the date in that format.
  */
object TsPartDayPartitioning extends PartitioningStrategy {
  final val name = TsPartDay.DimensionName

  override def partitioner(grid: Grid): Partitioner = {
    val partitions = for (
      (tsPartDay, _) <- grid.time.groupBy(t => TsPartDay.fromTime(t))
    ) yield tsPartDay -> Partition(s"$name=$tsPartDay")

    new TsPartDayPartitioner(partitions)
  }

  private class TsPartDayPartitioner(tsPartDayToPartition: Map[Long, Partition]) extends Partitioner {
    override def partitions: Iterable[Partition] = {
      tsPartDayToPartition.values
    }

    override def partition(coordinates: Coordinates): Partition = {
      tsPartDayToPartition(TsPartDay.fromTime(coordinates.time))
    }
  }
}
