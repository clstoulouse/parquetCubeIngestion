package fr.cls.bigdata.netcdf.chunking

import fr.cls.bigdata.georef.model.DimensionRef

final case class DimensionShape(ref: DimensionRef, totalSize: Int, chunkSize: Int) {
  def chunkCount: Int = Math.ceil(totalSize.toDouble / chunkSize).toInt
  def chunk(id: Int) = DimensionChunk(id * chunkSize, math.min((id + 1) * chunkSize, totalSize))
  def chunks: Seq[DimensionChunk] = {
    for (start <- Range(0, totalSize, chunkSize)) yield {
      val end = math.min(start + chunkSize, totalSize)
      DimensionChunk(start, end)
    }
  }
}
