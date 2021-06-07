package fr.cls.bigdata.metoc.parquet.writer.partition

import fr.cls.bigdata.metoc.model.Grid

/**
  * Objects that describes the strategy by which parquet files will be distributed on the storage.
  */
trait PartitioningStrategy {
  /**
    * @return Name of the partitioning strategy.
    */
  def name: String

  /**
    * Returns the partition of coordinates that should be placed in the same parquet folder
    *
    * @param grid     grid of coordinates of the datapoint.
    * @return all the partitions
    */
  def partitioner(grid: Grid): Partitioner
}

object PartitioningStrategy {
  /**
    * @param name Name of the partitioning strategy.
    * @return The partitioning strategy having the provided name, or `None` if no strategy with such a name exists.
    */
  def fromName(name: String): Option[PartitioningStrategy] = name match {
    case TsPartDayPartitioning.name => Some(TsPartDayPartitioning)
    case _ => None
  }
}