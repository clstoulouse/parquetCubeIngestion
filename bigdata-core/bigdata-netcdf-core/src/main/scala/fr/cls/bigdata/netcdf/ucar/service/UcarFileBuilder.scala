package fr.cls.bigdata.netcdf.ucar.service

import java.io.{File, IOException}

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, VariableMetadata}
import fr.cls.bigdata.georef.model.{DimensionRef, VariableRef}
import fr.cls.bigdata.netcdf.chunking.{DataShape, DimensionShape}
import fr.cls.bigdata.netcdf.exception.NetCDFException
import fr.cls.bigdata.netcdf.service.NetCDFFileBuilder
import fr.cls.bigdata.netcdf.ucar.internal.{DimensionWriterAccess, VariableWriterAccess}
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs.Path
import ucar.nc2.write.Nc4ChunkingDefault
import ucar.nc2.{NetcdfFileWriter, Variable}

class UcarFileBuilder(chunkSizeInBytes: Int) extends NetCDFFileBuilder {
  import fr.cls.bigdata.netcdf.ucar.internal.UcarMapper._

  import scala.collection.JavaConverters._

  private val chunkingStrategy = new Nc4ChunkingDefault()
  chunkingStrategy.setDefaultChunkSize(chunkSizeInBytes)

  override def create(metadata: DatasetMetadata, dimensionLengths: Map[DimensionRef, Int], path: Path): Resource[UcarFileWriter] = {
    try {
      val file = new File(path.toUri)
      for (fileWriter <- Resource(NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4, file.getAbsolutePath, chunkingStrategy))) yield {
        fileWriter.setFill(true) // fill variables with fill values according to the FillValue attribute
        for (attribute <- metadata.attributes) fileWriter.addGroupAttribute(null, toUcarAttribute(attribute))
        val dimensionAccesses = for ((ref, metadata) <- metadata.dimensions) yield {
          val length = dimensionLengths.getOrElse(ref, throw new NetCDFException(s"unknown length for $ref"))
          ref -> buildDimensionAccess(fileWriter, ref, metadata, length)
        }
        val variableAccesses = for ((ref, metadata) <- metadata.variables) yield {
          ref -> buildVariableAccess(fileWriter, dimensionAccesses, ref, metadata)
        }
        fileWriter.create()
        new UcarFileWriter(fileWriter, dimensionAccesses, variableAccesses)
      }
    } catch {
      case cause: IOException =>
        throw new NetCDFException(s"an error occurred while building netcdf file $path", cause)
    }
  }

  private def buildDimensionAccess(fileWriter: NetcdfFileWriter, ref: DimensionRef, metadata: DimensionMetadata, length: Int): DimensionWriterAccess = {
    val dimension = fileWriter.addDimension(null, metadata.shortName, length)
    val variable = fileWriter.addVariable(null, metadata.shortName, toUcarNumericType(metadata.dataType), metadata.shortName)
    val attributes = metadata.attributes.map(toUcarAttribute)
    variable.addAll(attributes.asJava)
    DimensionWriterAccess(ref, dimension, length, variable)
  }

  private def buildVariableAccess(fileWriter: NetcdfFileWriter, dimensionAccesses: Map[DimensionRef, DimensionWriterAccess], ref: VariableRef, metadata: VariableMetadata): VariableWriterAccess = {
    val variableDimensions = metadata.dimensions.map { dRef =>
      dimensionAccesses.getOrElse(dRef, throw new NetCDFException(s"unknown dimension $dRef"))
    }
    val variable = fileWriter.addVariable(null, metadata.shortName, toUcarNumericType(metadata.dataType), variableDimensions.map(_.dimension).asJava)
    val attributes = metadata.attributes.map(toUcarAttribute)
    variable.addAll(attributes.asJava)
    val shape = shapeOf(variable, variableDimensions)
    VariableWriterAccess(variable, shape)
  }

  private def shapeOf(variable: Variable, dimensions: Seq[DimensionWriterAccess]): DataShape = DataShape (
    for ((chunkSize, dimension) <- chunkingStrategy.computeChunking(variable).toSeq.zip(dimensions))
      yield DimensionShape(dimension.ref, dimension.length, chunkSize.toInt)
  )
}

object UcarFileBuilder {
  def apply(): UcarFileBuilder = new UcarFileBuilder(262144)
  def apply(chunkSizeInBytes: Int): UcarFileBuilder = new UcarFileBuilder(chunkSizeInBytes)
}
