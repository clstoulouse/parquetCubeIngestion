package fr.cls.bigdata.metoc.ingestion

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model._
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.metoc.ingestion.internal.NormalizedGrid
import fr.cls.bigdata.metoc.model._
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape, DimensionShape}
import fr.cls.bigdata.netcdf.model.{NetCDFDimension, NetCDFVariable}

trait TestData {
  val attribute = MetadataAttribute("attribute", DataType.String, Seq("value"))

  object Longitude {
    val ref: DimensionRef = Dimensions.longitude
    val shortName = "lon"
    val dataType: DataType = DataType.Double
    val length = 3
    val storage: DataStorage = dataType.store(Seq(-180D, 0D, 180D))
    val actualValues: IndexedSeq[Option[Double]] = IndexedSeq(Some(-180D), Some(0D), None)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(attribute))
    val netcdf = NetCDFDimension(ref, metadata)
  }

  object Latitude {
    val ref: DimensionRef = Dimensions.latitude
    val shortName = "lat"
    val dataType: DataType = DataType.Float
    val length = 2
    val storage: DataStorage = dataType.store(Seq(90F, -90F))
    val actualValues: IndexedSeq[Double] = IndexedSeq(90D, -90D)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(attribute))
    val netcdf = NetCDFDimension(ref, metadata)
  }

  object Time {
    val shortName = "time"
    val dataType: DataType = DataType.Long
    val ref: DimensionRef = Dimensions.time
    val length = 1
    val storage: DataStorage = dataType.store(Seq(2L))
    val actualValues: Vector[Long] = Vector(172800000L)
    val shape = DimensionShape(ref, length, length)
    val unit = MetadataAttribute("units", DataType.String, Seq("days since 1970-01-01 00:00:00"))
    val metadata = DimensionMetadata(shortName, dataType, Set(attribute, unit))
    val netcdf = NetCDFDimension(ref, metadata)
  }

  object Depth {
    val shortName = "depth"
    val ref: DimensionRef = Dimensions.depth
    val standardName: String = ref.name
    val dataType: DataType = DataType.Int
    val length = 1
    val storage: DataStorage = dataType.store(Seq(2))
    val actualValues: Vector[Double] = Vector(2D)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(attribute))
    val netcdf = NetCDFDimension(ref, metadata)
  }

  val globalAttributes: Set[MetadataAttribute] = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3)))
  val defaultRounding = Rounding(precision = 2, Rounding.RoundHalfUp)
  val noVariables = Set.empty[String]
  val noMetadata = DatasetMetadata(Map(), Map(), Set())

  object Variable3D {
    val shortName = "var3d"
    val ref = VariableRef("variable_3d")
    val dataType: DataType = DataType.Double
    val shape = DataShape(Seq(Time.shape, Latitude.shape, Longitude.shape))
    val metadata = VariableMetadata(shortName, dataType, Seq(Dimensions.time, Dimensions.latitude, Dimensions.longitude), Set(attribute))
    val netcdf = NetCDFVariable(ref, shape, metadata)
    val storage: DataStorage = dataType.store(Seq(10D, 11D, 12D, 13D, 14D, 15D))
    val chunk: DataChunk = shape.chunks.head
  }

  object Variable4D {
    val shortName = "var4d"
    val ref = VariableRef("variable_4d")
    val dataType: DataType = DataType.Double
    val shape = DataShape(Seq(Time.shape, Depth.shape, Latitude.shape, Longitude.shape))
    val metadata = VariableMetadata(shortName, dataType, Seq(Dimensions.time, Dimensions.depth, Dimensions.latitude, Dimensions.longitude), Set(attribute))
    val netcdf = NetCDFVariable(ref, shape, metadata)
    val storage: DataStorage = dataType.store(Seq(4D, 5D, 6D, 7D, 8D, 9D))
    val chunk: DataChunk = shape.chunks.head
  }

  object Only3D {
    val netCDFDimensions: Seq[NetCDFDimension] = Seq(
      Longitude.netcdf,
      Latitude.netcdf,
      Time.netcdf
    )
    val dimensions: Set[DimensionRef] = netCDFDimensions.map(_.ref).toSet
    val grid = NormalizedGrid(Longitude.actualValues, Latitude.actualValues, Time.actualValues, Vector.empty)
    val variables: Seq[NetCDFVariable] = Seq(Variable3D.netcdf)
    val dataPoints: Seq[DataPoint] = Seq(
      DataPoint(Coordinates(-180D, 90D, 172800000L, None), Seq(Variable3D.ref -> Some(10D))),
      DataPoint(Coordinates(0D, 90D, 172800000L, None), Seq(Variable3D.ref -> Some(11D))),
      DataPoint(Coordinates(-180D, -90D, 172800000L, None), Seq(Variable3D.ref -> Some(13D))),
      DataPoint(Coordinates(0D, -90D, 172800000L, None), Seq(Variable3D.ref -> Some(14D)))
    )
  }

  object Mixed3DAnd4D {
    val netCDFDimensions: Seq[NetCDFDimension] = Only3D.netCDFDimensions :+ Depth.netcdf
    val dimensions: Set[DimensionRef] = netCDFDimensions.map(_.ref).toSet
    val grid = NormalizedGrid(Longitude.actualValues, Latitude.actualValues, Time.actualValues, Depth.actualValues)
    val variables: Seq[NetCDFVariable] = Seq(Variable3D.netcdf, Variable4D.netcdf)
    val dataPoints: Seq[DataPoint] = Seq(
      DataPoint(Coordinates(-180D, 90D, 172800000L, Some(2D)), Seq(Variable4D.ref -> Some(4D))),
      DataPoint(Coordinates(0D, 90D, 172800000L, Some(2D)), Seq(Variable4D.ref -> Some(5D))),
      DataPoint(Coordinates(-180D, -90D, 172800000L, Some(2D)), Seq(Variable4D.ref -> Some(7D))),
      DataPoint(Coordinates(0D, -90D, 172800000L, Some(2D)), Seq(Variable4D.ref -> Some(8D))),

      DataPoint(Coordinates(-180D, 90D, 172800000L, None), Seq(Variable3D.ref -> Some(10D))),
      DataPoint(Coordinates(0D, 90D, 172800000L, None), Seq(Variable3D.ref -> Some(11D))),
      DataPoint(Coordinates(-180D, -90D, 172800000L, None), Seq(Variable3D.ref -> Some(13D))),
      DataPoint(Coordinates(0D, -90D, 172800000L, None), Seq(Variable3D.ref -> Some(14D)))
    )
  }
}
