package fr.cls.bigdata.netcdf.ucar.service

 import java.io.IOException

import fr.cls.bigdata.georef.metadata.{DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataStorage, DimensionRef, VariableRef}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape, DimensionShape}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.netcdf.model.{NetCDFDimension, NetCDFVariable}
import fr.cls.bigdata.netcdf.service.NetCDFFileReader
import fr.cls.bigdata.netcdf.ucar.internal.{DimensionReaderAccess, HDFSRandomAccessFile, VariableReaderAccess}
import fr.cls.bigdata.resource.Resource
import ucar.nc2.{Attribute, NetcdfFile, Variable}

class UcarFileReader(override val file: PathWithFileSystem,
                     override val globalAttributes: Set[MetadataAttribute],
                     dimensionAccesses: Map[DimensionRef, DimensionReaderAccess],
                     variableAccesses: Map[VariableRef, VariableReaderAccess]) extends NetCDFFileReader {

  override val dimensions: Seq[NetCDFDimension] = dimensionAccesses.values.map(d => NetCDFDimension(d.ref, d.metadata)).toSeq
  override val variables: Seq[NetCDFVariable] = variableAccesses.values.map(v => NetCDFVariable(v.ref, v.shape, v.metadata)).toSeq

  override def read(ref: DimensionRef): DataStorage = {
    dimensionAccesses.getOrElse(ref, throw new NetCDFException(s"invalid dimension $ref in $file")).read
  }

  override def read(ref: VariableRef, chunk: DataChunk): DataStorage = {
    variableAccesses.getOrElse(ref, throw new NetCDFException(s"invalid variable $ref in $file")).read(chunk)
  }
}

object UcarFileReader {
  import fr.cls.bigdata.georef.metadata.Constants._
  import fr.cls.bigdata.netcdf.ucar.internal.UcarMapper._

  import scala.collection.JavaConverters._

  def apply(file: PathWithFileSystem): Resource[NetCDFFileReader] = {
    this.apply(file, Set.empty)
  }

  def apply(file: PathWithFileSystem, variablesToExclude: Set[String]): Resource[NetCDFFileReader] = {
    for(netCDFFile <- open(file)) yield UcarFileReader(file, netCDFFile, variablesToExclude)
  }


  private def open(file: PathWithFileSystem): Resource[NetcdfFile] = Resource {
    val scheme = file.path.toUri.getScheme
    if (scheme.startsWith("hdfs")) {
      NetcdfFile.open(HDFSRandomAccessFile(file), file.toString(), null, null)
    } else {
      NetcdfFile.open(file.toString())
    }
  }

  /**
    * Reads the standardDimensions and variables of the NetCDF file to create an instance of
    * [[fr.cls.bigdata.netcdf.service.NetCDFFileReader]]
    *
    * @param netCDFFile A [[ucar.nc2.NetcdfFile]] that must be closed after usage
    * @throws NetCDFException when a IOException occurs
    * @return the Metoc NetCDF file
    */
  @throws[NetCDFException]
  def apply(file: PathWithFileSystem, netCDFFile: NetcdfFile, variablesToExclude: Set[String]): NetCDFFileReader = {
    try {
      val globalAttributes = for (ncAttribute <- netCDFFile.getGlobalAttributes.asScala.toSet[Attribute]) yield toAttribute(ncAttribute)
      val standardDimensions = readStandardDimensions(netCDFFile)
      val variables = readVariables(netCDFFile, standardDimensions, variablesToExclude)

      new UcarFileReader(file, globalAttributes, standardDimensions.map(d => d.ref -> d).toMap, variables.map(v => v.ref -> v).toMap)
    } catch {
      case cause: IOException =>
        throw new NetCDFException(s"An IOException occurred while reading in a NetCDF file", cause)
    }

  }

  @throws[NetCDFException]
  def apply(file: PathWithFileSystem, netCDFFile: NetcdfFile): NetCDFFileReader = {
    this.apply(file, netCDFFile, Set.empty)
  }

  /**
    * Reads all the dimension in the NetCDF file.
    * If a dimension has no standard name it is ignored.
    *
    * @param file The NetCDF file.
    * @return the standard dimension access
    */
  private def readStandardDimensions(file: NetcdfFile): Seq[DimensionReaderAccess] = {
    for {
      dimension <- file.getDimensions.asScala
      variable = file.findVariable(dimension.getFullName)
      attributes = readAttributes(variable)
      dataType = toMetocType(variable.getDataType)
      metadata = DimensionMetadata(dimension.getShortName, dataType, attributes)
      if isStandardDimension(metadata)
    } yield DimensionReaderAccess(metadata, variable)
  }

  private def isStandardDimension(metadata: DimensionMetadata): Boolean = {
    metadata.findSingleValue[String](StandardNameAttribute).exists(_.nonEmpty)
  }

  /**
    * Reads all the variables of the NetCDF file
    * Variables with dimension that are not standard are ignored
    *
    * @param file       The NetCDF standardDimensions.
    * @param standardDimensions The standard standardDimensions.
    * @return (a map: var ref -> metadata, a map: var ref -> data access) the two maps contain the same keys
    */
  private def readVariables(file: NetcdfFile, standardDimensions: Seq[DimensionReaderAccess], variablesToExclude: Set[String]): Set[VariableReaderAccess] = {
    for {
      variable <- file.getVariables.asScala.toSet.filterNot(v => variablesToExclude.contains(v.getShortName)) if standardDimensions.forall(d => d.metadata.shortName != variable.getShortName)
      variableDimensions <- getVariableDimensions(variable, standardDimensions)
    } yield {
      val shape =  getShape(variable, variableDimensions)
      val dataType = toMetocType(variable.getDataType)
      val attributes = readAttributes(variable)
      val metadata = VariableMetadata(variable.getShortName, dataType, variableDimensions.map(_.ref), attributes)
      VariableReaderAccess(metadata, shape, variable)
    }
  }

  /**
  def getVariableRef(variable: Variable) : VariableRef = {
    variable.
    /**filter(_.nonEmpty)
    .orElse(metadata.findAttrSingleValue(LongNameAttribute)).filter(_.nonEmpty)
    .getOrElse(metadata.shortName).replace(' ','_')
     */
  }
*/
  /**
    * Returns a sequence of the variable standardDimensions.
    * Returns None if the variable has a dimension that is not standard
    *
    * @param variable           the ucar variable.
    * @param dimensions         the dimensions of the variable
    * @return                   the ordered sequence of standardDimensions
    */
  private def getVariableDimensions(variable: Variable, dimensions: Seq[DimensionReaderAccess]): Option[Seq[DimensionReaderAccess]] = {
    val standardDimensions = for (ucarDimension <- variable.getDimensions.asScala)
      yield dimensions.find(d => d.metadata.shortName == ucarDimension.getShortName)

    standardDimensions.foldLeft(Option(Seq[DimensionReaderAccess]())){
      case (Some(seq), Some(dim)) => Some(seq :+ dim)
      case _ => None
    }
  }

  /**
    * Returns the shape of the variable data
    *
    * @param variable the ucar variable
    * @param dimensions the dimensions of the variables
    * @return the shape of the variable data
    */
  private def getShape(variable: Variable, dimensions: Seq[DimensionReaderAccess]): DataShape = {
    val dimensionShapes = Option(variable.findAttribute(ChunkSizeAttribute)) match {
      case None => dimensions.map(d => DimensionShape(d.ref, d.totalSize, d.totalSize))
      case Some(chunkSizeAttribute)  =>
        val chunkSizes = chunkSizeAttribute.getValues
        dimensions.zipWithIndex.map { case (dimension, index) =>
          val chunkSize = chunkSizes.getInt(index)
          DimensionShape(dimension.ref, dimension.totalSize, chunkSize)
        }
    }
    DataShape(dimensionShapes)
  }

  private def readAttributes(variable: Variable): Set[MetadataAttribute] = {
    for { ncAttribute <- variable.getAttributes.asScala.toSet[Attribute] } yield toAttribute(ncAttribute)
  }

  private def toAttribute(ncAttribute: Attribute): MetadataAttribute = {
    val fullName = ncAttribute.getFullName
    val dataType = toMetocType(ncAttribute.getDataType)
    val values = for (i <- 0 until ncAttribute.getLength) yield ncAttribute.getValue(i)
    MetadataAttribute(fullName, dataType, values)
  }



}
