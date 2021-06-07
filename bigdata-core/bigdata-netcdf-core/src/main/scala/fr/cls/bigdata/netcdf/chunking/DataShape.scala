package fr.cls.bigdata.netcdf.chunking

import fr.cls.bigdata.georef.model.Dimensions

final case class DataShape(dimensions: Seq[DimensionShape]) {
  def chunkCount: Int = dimensions.map(_.chunkCount).product

  def chunks: Seq[DataChunk] = {
    dimensions.foldLeft(Seq(Seq.empty[DimensionChunk])){ (seq, axis) =>
      seq.view.flatMap(chunk => axis.chunks.view.map(chunk :+ _))
    }.map(DataChunk)
  }

  def chunk(id: Int): DataChunk = {
    val (dims, _) = dimensions.foldRight((Seq.empty[DimensionChunk], id)) {
      case (axis, (seq, acc)) =>
        (axis.chunk(acc % axis.chunkCount) +: seq, acc / axis.chunkCount)
    }
    DataChunk(dims)
  }

  def is3D: Boolean = dimensions.map(_.ref).toSet == Dimensions.`3D`
  def is4D: Boolean = dimensions.map(_.ref).toSet == Dimensions.`4D`
}
