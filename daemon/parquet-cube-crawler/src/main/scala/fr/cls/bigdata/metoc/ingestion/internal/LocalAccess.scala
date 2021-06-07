package fr.cls.bigdata.metoc.ingestion.internal

import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.metoc.model.DataPoint
import fr.cls.bigdata.metoc.service.DataAccess
import fr.cls.bigdata.netcdf.chunking.DataShape
import fr.cls.bigdata.netcdf.conversion.VariableConverter
import fr.cls.bigdata.netcdf.model.NetCDFVariable
import fr.cls.bigdata.netcdf.service.NetCDFFileReader

class LocalAccess(file: NetCDFFileReader, normalizedGrid: NormalizedGrid, selectedVariables: Seq[NetCDFVariable])
  extends DataAccess[Iterator[DataPoint]] {
  def get: Iterator[DataPoint] = {
    for {
      (shape, variableGroup) <- selectedVariables.groupBy(_.shape).iterator
      point <- read(shape, variableGroup)
    } yield point
  }

  private def read(shape: DataShape, variables: Seq[NetCDFVariable]): Iterator[DataPoint] = {
    val allValues = readAllValues(shape, variables)
    val coordinates = normalizedGrid.coordinates(shape)

    // zip coordinates and values, then remove None coordinates, that represents duplicate grid coordinates
    coordinates.zip(allValues).collect {
      case (Some(coords), values) if values.exists { case (_, value) => value.isDefined } => DataPoint(coords, values)
    }
  }

  private def readAllValues(shape: DataShape, variables: Seq[NetCDFVariable]): Iterator[Seq[(VariableRef, Option[Double])]] = {
    val variablesValues = for (variable <- variables) yield readVariableValues(shape, variable)
    new LocalAccess.ZippedIterator(variablesValues)
  }

  private def readVariableValues(shape: DataShape, variable: NetCDFVariable): Iterator[(VariableRef, Option[Double])] = {
    val converter = VariableConverter(variable.metadata)
    for {
      chunk <- shape.chunks.iterator
      rawValue <- file.read(variable.ref, chunk).iterator
    } yield variable.ref -> converter.fromNetCDF(rawValue)
  }
}

object LocalAccess {
  private class ZippedIterator[T](iterators: Seq[Iterator[T]]) extends Iterator[Seq[T]] {
    override def hasNext: Boolean = iterators.forall(_.hasNext)

    override def next(): Seq[T] = iterators.map(_.next)
  }
}
