package fr.cls.bigdata.netcdf.model

import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}
import fr.cls.bigdata.georef.metadata.VariableMetadata
import fr.cls.bigdata.netcdf.chunking.DataShape

final  case class NetCDFVariable(ref: VariableRef, shape: DataShape, metadata: VariableMetadata) {
  def dataType: DataType = metadata.dataType
  def dimensions: Seq[DimensionRef] = shape.dimensions.map(_.ref)
}
