package fr.cls.bigdata.metoc.parquet.writer.services

import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.metoc.parquet.writer.partition.{Partition, Partitioner, PartitioningStrategy}

final case class PartitioningStrategyMock(partitionToCoordinates : (Partition, Seq[Coordinates])*) extends PartitioningStrategy {
  val name: String = "mock partitioning strategy"

  /**
    * Returns the partition of coordinates that should be placed in the same parquet folder
    *
    * @param grid grid of coordinates of the datapoint.
    * @return all the partitions
    */
  override def partitioner(grid: Grid): Partitioner = PartitionerMock

  private object PartitionerMock extends Partitioner {
    override def partitions: Iterable[Partition] = partitionToCoordinates.map(_._1)

    override def partition(coords: Coordinates): Partition = {
      partitionToCoordinates.find( _._2.contains(coords)).get._1
    }
  }
}
