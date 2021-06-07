package fr.cls.bigdata.metoc.parquet.utils

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, VariableRef}
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import scala.collection.mutable

trait MetocParquetTestUtils {
  import fr.cls.bigdata.georef.model.Dimensions._

  // example variables
  final val Var3d_1 = VariableRef(name = "var_3d_1")
  final val Var3d_2 = VariableRef(name = "var_3d_2")
  final val Var4d_1 = VariableRef(name = "var_4d_1")
  final val Var4d_2 = VariableRef(name = "var_4d_2")


  // example dimensions values
  final val Time1 = 1550102384000L
  final val Time2 = 1550102401000L

  final val Lat1 = 90D
  final val Lat2 = -12D

  final val Lon1 = 150D
  final val Lon2 = -59D

  final val Depth1 = 0
  final val Depth2 = 50D

  // example dataset specs
  final val variable3Ds = Set(Var3d_1, Var3d_2)
  final val variable3DsMetadata = DatasetMetadata(
    Map(
      longitude -> DimensionMetadata(longitude.name, DataType.Double),
      latitude -> DimensionMetadata(latitude.name, DataType.Double),
      time -> DimensionMetadata(time.name, DataType.Long)
    ),
    Map(
      Var3d_1 -> VariableMetadata(Var3d_1.name, DataType.Double, Seq(time, latitude, longitude)),
      Var3d_2 -> VariableMetadata(Var3d_1.name, DataType.Double, Seq(time, latitude, longitude))
    ),
    Set()
  )

  final val variable4Ds = Set(Var4d_1, Var4d_2)
  final val variable4DsMetadata = DatasetMetadata(
    Map(
      longitude -> DimensionMetadata(longitude.name, DataType.Double),
      latitude -> DimensionMetadata(latitude.name, DataType.Double),
      time -> DimensionMetadata(time.name, DataType.Long),
      depth -> DimensionMetadata(depth.name, DataType.Double)
    ),
    Map(
      Var4d_1 -> VariableMetadata(Var4d_1.name, DataType.Double, Seq(time, latitude, longitude, depth)),
      Var4d_2 -> VariableMetadata(Var4d_2.name, DataType.Double, Seq(time, latitude, longitude, depth))
    ),
    Set()
  )

  final val mixed3DAnd4DVariables = Set(Var3d_1, Var3d_2, Var4d_1, Var4d_2)
  final val mixedMetadata = DatasetMetadata(
    Map(
      longitude -> DimensionMetadata(longitude.name, DataType.Double),
      latitude -> DimensionMetadata(latitude.name, DataType.Double),
      time -> DimensionMetadata(time.name, DataType.Long),
      depth -> DimensionMetadata(depth.name, DataType.Double)
    ),
    Map(
      Var3d_1 -> VariableMetadata(Var3d_1.name, DataType.Double, Seq(time, latitude, longitude)),
      Var3d_2 -> VariableMetadata(Var3d_1.name, DataType.Double, Seq(time, latitude, longitude)),
      Var4d_1 -> VariableMetadata(Var4d_1.name, DataType.Double, Seq(time, latitude, longitude, depth)),
      Var4d_2 -> VariableMetadata(Var4d_2.name, DataType.Double, Seq(time, latitude, longitude, depth))
    ),
    Set()
  )

  // example avro schema
  private lazy val avroSchemaParser = new Schema.Parser()

  final val AvroSchema3dOnly = avroSchemaParser.parse(
    """
      |{
      |  "type" : "record",
      |  "name" : "netcdfAvro1",
      |  "namespace" : "fr.cls.parquet",
      |  "fields" : [ {
      |    "name" : "time",
      |    "type" : "long"
      |  }, {
      |    "name" : "longitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "latitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "var_3d_1",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_3d_2",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  } ]
      |}
    """.stripMargin)

  final val AvroSchema4dOnly = avroSchemaParser.parse(
    """
      |{
      |  "type" : "record",
      |  "name" : "netcdfAvro2",
      |  "namespace" : "fr.cls.parquet",
      |  "fields" : [ {
      |    "name" : "time",
      |    "type" : "long"
      |  }, {
      |    "name" : "longitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "latitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "depth",
      |    "type" : "double"
      |  }, {
      |    "name" : "var_4d_1",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_4d_2",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  } ]
      |}
    """.stripMargin)

  final val AvroSchemaMixed3dAnd4d = avroSchemaParser.parse(
    """
      |{
      |  "type" : "record",
      |  "name" : "netcdfAvro3",
      |  "namespace" : "fr.cls.parquet",
      |  "fields" : [ {
      |    "name" : "time",
      |    "type" : "long"
      |  }, {
      |    "name" : "longitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "latitude",
      |    "type" : "double"
      |  }, {
      |    "name" : "depth",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_3d_1",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_3d_2",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_4d_1",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  }, {
      |    "name" : "var_4d_2",
      |    "type" : [ "null", "double" ],
      |    "default" : null
      |  } ]
      |}
    """.stripMargin)

  // -----------------------------------------------------------------------
  case class ParquetRecord(fields: Map[String, Option[AnyRef]]) {
    def hasField(name: String): Boolean = fields.contains(name)

    def get(name: String): Option[AnyRef] = fields.getOrElse(name, None)
  }

  def readParquetFileRecords(parquetFile: PathWithFileSystem): Seq[ParquetRecord] = {
    val buffer = mutable.ListBuffer.empty[ParquetRecord]

    val parquetReader = AvroParquetReader
      .builder[GenericRecord](HadoopInputFile.fromPath(parquetFile.path, HadoopTestUtils.HadoopConfiguration))
      .disableCompatibility()
      .build()

    try {
      var proceed = true
      while (proceed) {
        val record = parquetReader.read()
        if (record == null) {
          proceed = false
        } else {
          import scala.collection.JavaConverters._
          val recordSchema = record.getSchema
          val fields = recordSchema.getFields.asScala.map { field =>
            val name = field.name()
            val value = Option(record.get(name))
            (name, value)
          }.toMap
          buffer += ParquetRecord(fields)
        }
      }

      buffer.toList
    } finally {
      parquetReader.close()
    }

  }
}
