package fr.cls.bigdata.netcdf

import fr.cls.bigdata.georef.metadata._
import fr.cls.bigdata.georef.model.{DataStorage, DataType, DimensionRef, VariableRef}
import fr.cls.bigdata.netcdf.chunking.{DataChunk, DataShape, DimensionShape}
import fr.cls.bigdata.netcdf.model.NetCDFVariable

trait TestData {
  val someAttribute = MetadataAttribute("attribute", DataType.String, Seq("value"))

  object Longitude {
    val shortName = "lon"
    val standardName: String = "longitude"
    val ref = DimensionRef(standardName)
    val dataType: DataType = DataType.Double
    val length = 3
    val rawData: DataStorage = dataType.store(Seq(-180D, 0D, 180D))
    val actualValues: Vector[Option[Double]] = Vector(Some(-180D), Some(0D), None)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(someAttribute))
  }

  object Latitude {
    val shortName = "lat"
    val standardName: String = "latitude"
    val ref = DimensionRef(standardName)
    val dataType: DataType = DataType.Float
    val length = 2
    val rawData: DataStorage = dataType.store(Seq(90F, -90F))
    val actualValues: Vector[Double] = Vector(90D, -90D)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(someAttribute))
  }

  object Time {
    val shortName = "time"
    val standardName: String = "time"
    val ref = DimensionRef(standardName)
    val dataType: DataType = DataType.Long
    val length = 1
    val rawData: DataStorage = dataType.store(Seq(2L))
    val actualValues: Vector[Long] = Vector(172800000L)
    val shape = DimensionShape(ref, length, length)
    val unit = MetadataAttribute("units", DataType.String, Seq("days since 1970-01-01 00:00:00"))
    val metadata = DimensionMetadata(shortName, dataType, Set(someAttribute, unit))
  }

  object Depth {
    val shortName = "depth"
    val standardName: String = shortName
    val ref = DimensionRef(standardName)
    val dataType: DataType = DataType.Int
    val length = 1
    val rawData: DataStorage = dataType.store(Seq(2))
    val actualValues: Vector[Double] = Vector(2D)
    val shape = DimensionShape(ref, length, length)
    val metadata = DimensionMetadata(shortName, dataType, Set(someAttribute))
  }

  val globalAttributes: Set[MetadataAttribute] = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3)))
  val noVariables = Set.empty[VariableRef]
  val noMetadata = DatasetMetadata(Map(), Map(), Set())

  object Variable3D {
    val shortName = "var3d"
    val ref = VariableRef("variable_3d")
    val dataType: DataType = DataType.Double
    val shape = DataShape(Seq(Time.shape, Latitude.shape, Longitude.shape))
    val fillValue = 100D
    val fillValueAttribute = MetadataAttribute(Constants.FillValueAttribute, dataType, Seq(fillValue))
    val metadata = VariableMetadata(shortName, dataType, Seq(Time.ref, Latitude.ref, Longitude.ref), Set(someAttribute, fillValueAttribute))
    val netcdf = NetCDFVariable(ref, shape, metadata)
    val rawData: DataStorage = dataType.store(Seq(10D, 11D, 12D, 13D, 14D, 15D))
    val chunk: DataChunk = shape.chunks.head
  }

  object Variable4D {
    val shortName = "var4d"
    val ref = VariableRef("variable_4d")
    val dataType: DataType = DataType.Double
    val shape = DataShape(Seq(Time.shape, Depth.shape, Latitude.shape, Longitude.shape))
    val metadata = VariableMetadata(shortName, dataType, Seq(Time.ref, Depth.ref, Latitude.ref, Longitude.ref), Set(someAttribute))
    val netcdf = NetCDFVariable(ref, shape, metadata)
    val rawData: DataStorage = dataType.store(Seq(4D, 5D, 6D, 7D, 8D, 9D))
    val chunk: DataChunk = shape.chunks.head
  }

  object Only3D {
    val dimensions: Map[DimensionRef, DimensionMetadata] = Map(
      Longitude.ref -> Longitude.metadata,
      Latitude.ref -> Latitude.metadata,
      Time.ref -> Time.metadata
    )
    val variables: Seq[NetCDFVariable] = Seq(Variable3D.netcdf)
  }

  object Mixed3DAnd4D {
    val dimensions: Map[DimensionRef, DimensionMetadata] = Only3D.dimensions + (Depth.ref -> Depth.metadata)
    val variables: Seq[NetCDFVariable] = Seq(Variable3D.netcdf, Variable4D.netcdf)
  }
}
