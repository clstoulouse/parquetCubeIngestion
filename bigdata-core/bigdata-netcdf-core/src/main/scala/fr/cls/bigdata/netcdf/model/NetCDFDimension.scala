package fr.cls.bigdata.netcdf.model

import fr.cls.bigdata.georef.metadata.DimensionMetadata
import fr.cls.bigdata.georef.model.{DataType, DimensionRef}

final case class NetCDFDimension(ref: DimensionRef, metadata: DimensionMetadata) {
  def dataType: DataType = metadata.dataType
}
