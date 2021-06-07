package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.metadata.DimensionMetadata
import fr.cls.bigdata.georef.model.{DataStorage, DimensionRef}
import ucar.nc2.Variable

private[ucar] case class DimensionReaderAccess(metadata: DimensionMetadata, variable: Variable) {
  import fr.cls.bigdata.georef.metadata.Constants._

  val totalSize: Int = variable.getSize.toInt

  val ref: DimensionRef = DimensionRef(
    metadata.findSingleValue[String](StandardNameAttribute).filter(_.nonEmpty)
      .orElse(metadata.findSingleValue[String](LongNameAttribute)).filter(_.nonEmpty)
      .getOrElse(metadata.shortName)
  )

  def read: DataStorage = DataStorage(metadata.dataType, variable.read().copyTo1DJavaArray())
}
