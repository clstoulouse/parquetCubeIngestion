package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.model.{DataStorage, VariableRef}
import fr.cls.bigdata.georef.metadata.VariableMetadata
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import ucar.ma2.Range
import ucar.nc2.Variable

private[ucar] final case class VariableReaderAccess(metadata: VariableMetadata, shape: DataShape, variable: Variable) {
  import scala.collection.JavaConverters._
  import fr.cls.bigdata.georef.metadata.Constants._

  val ref: VariableRef = VariableRef(
    metadata.findAttrSingleValue[String](StandardNameAttribute).filter(_.nonEmpty)
      .orElse(metadata.findAttrSingleValue(LongNameAttribute)).filter(_.nonEmpty)
      .getOrElse(metadata.shortName).replace(' ','_').replace('=','_')
  )

  @throws[NetCDFException]
  def read(chunk: DataChunk): DataStorage = {
    val chunkData = variable.read(chunk.dimensions.map(d => new Range(d.start, d.end - 1)).asJava).copyTo1DJavaArray()
    DataStorage(metadata.dataType, chunkData)
  }
}
