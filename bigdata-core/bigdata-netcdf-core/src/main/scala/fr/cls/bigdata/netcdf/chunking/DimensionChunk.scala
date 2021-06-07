package fr.cls.bigdata.netcdf.chunking

/**
  *
  * @param start start index (inclusive)
  * @param end end index (exclusive)
  */
final case class DimensionChunk(start: Int, end: Int) {
  val range: Range = start until end
  val size: Int = end - start
}
