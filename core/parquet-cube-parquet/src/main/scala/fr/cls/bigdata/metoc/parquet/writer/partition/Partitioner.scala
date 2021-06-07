package fr.cls.bigdata.metoc.parquet.writer.partition

import fr.cls.bigdata.metoc.model.Coordinates

trait Partitioner {
  def partitions: Iterable[Partition]
  def partition(coords: Coordinates): Partition
}
