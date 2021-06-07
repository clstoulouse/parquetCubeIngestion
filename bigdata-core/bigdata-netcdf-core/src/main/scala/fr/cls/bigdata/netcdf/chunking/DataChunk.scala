package fr.cls.bigdata.netcdf.chunking

case class DataChunk(dimensions: Seq[DimensionChunk]){
  val size: Int = dimensions.map(_.size).product
  val range: Range = 0 until size

  def coordinates: Iterator[Seq[Int]] = {
    dimensions.reverse.foldRight(Iterator(Seq.empty[Int])) {
      (dimensionChunk, iterator) =>
        for {
          coords <- iterator
          i <- dimensionChunk.range
        } yield i +: coords
    }.map(_.reverse)
  }
}
