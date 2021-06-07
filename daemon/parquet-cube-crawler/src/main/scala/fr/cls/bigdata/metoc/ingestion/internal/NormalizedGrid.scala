package fr.cls.bigdata.metoc.ingestion.internal

import fr.cls.bigdata.georef.model.{DimensionRef, Dimensions}
import fr.cls.bigdata.georef.utils.{Normalizer, Rounding}
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import fr.cls.bigdata.netcdf.chunking.DataShape
import fr.cls.bigdata.netcdf.conversion.DimensionConverter
import fr.cls.bigdata.netcdf.model.NetCDFDimension
import fr.cls.bigdata.netcdf.service.NetCDFFileReader

final case class NormalizedGrid(longitudes: IndexedSeq[Option[Double]],
                                                 latitudes: IndexedSeq[Double],
                                                 times: IndexedSeq[Long],
                                                 depths: IndexedSeq[Double]) {

  val toMetoc: Grid = Grid.create(longitudes.flatten, latitudes, times, depths)

  /** Iterate on the coordinates of the netcdf shape, some coordinates are ignored because their longitude is a duplicate.
    * Ex: When longitude -180 and longitude 180 are present in the same netcdf file, the second one is ignored
    *
    * @param shape the shape of the data in the netcdf, which is composed of the chunk size of each dimension
    * @return None in case of duplicated coordinates, Some[Coordinates] otherwise
    */
  def coordinates(shape: DataShape): Iterator[Option[Coordinates]] = {
    val dimensionsPositions = shape.dimensions.map(_.ref).zipWithIndex.toMap
    val longitudePos = dimensionsPositions(Dimensions.longitude)
    val latitudePos =  dimensionsPositions(Dimensions.latitude)
    val timePos = dimensionsPositions(Dimensions.time)
    val depthPosOpt = dimensionsPositions.get(Dimensions.depth)

    for {
      chunk <- shape.chunks.iterator
      indices <- chunk.coordinates
    } yield coordinates(indices(longitudePos), indices(latitudePos), indices(timePos), depthPosOpt.map(indices))
  }

  private def coordinates(lonIdx: Int, latIdx: Int, timeIdx: Int, depthIdxOpt: Option[Int]): Option[Coordinates] = {
    for (longitude <- longitudes(lonIdx)) // return None in case the longitude is a duplicate
      yield Coordinates(longitude, latitudes(latIdx), times(timeIdx), depthIdxOpt.map(depths))
  }
}

object NormalizedGrid {
  def apply(file: NetCDFFileReader, rounding: Rounding): NormalizedGrid = {
    val dimensions = file.dimensions.map(_.ref).toSet

    dimensions.diff(Dimensions.`4D`).foreach { d =>
      throw new MetocReaderException(s"Unknown dimension $d in NetCDF file")
    }

    val longitudeValues = removeDuplicate(read[Double](file, Dimensions.longitude, Normalizer.Longitude(rounding)))
    val latitudeValues = read[Double](file, Dimensions.latitude, Normalizer.Latitude(rounding))
    val timeValues = read[Long](file, Dimensions.time, Normalizer.None[Long]())
    val depthValues = dimensions.find(_ == Dimensions.depth)
      .map(read[Double](file, _, Normalizer.None[Double]()))
      .getOrElse(IndexedSeq())

    NormalizedGrid(longitudeValues, latitudeValues, timeValues, depthValues)
  }

  private def read[T](file: NetCDFFileReader, ref: DimensionRef, normalizer: Normalizer[T]): IndexedSeq[T] = {
    val dimension = file.dimensions.find(d => d.ref == ref).getOrElse {
      throw new MetocReaderException(s"Missing $ref in NetCDF file")
    }
    read(file, dimension, normalizer)
  }

  private def read[T](file: NetCDFFileReader, dimension: NetCDFDimension, normalizer: Normalizer[T]): IndexedSeq[T] = {
    val converter = DimensionConverter(dimension)
    val normalizedValues = file.read(dimension.ref).view
      .map(value => converter.fromNetCDF(value).asInstanceOf[T])
      .map(normalizer.normalize)
    IndexedSeq(normalizedValues: _*)
  }

  /**
    * Replace the duplicated values with None
    *
    * @param values the original values
    * @return the values in order where the duplicates are replaced with None
    */
  private def removeDuplicate(values: IndexedSeq[Double]): IndexedSeq[Option[Double]] = {
    values.foldLeft((Set.empty[Double], IndexedSeq.empty[Option[Double]])) { case ((alreadySeen, result), value) =>
      if (alreadySeen.contains(value))  (alreadySeen, result :+ None) else (alreadySeen + value, result :+ Some(value))
    }._2
  }
}