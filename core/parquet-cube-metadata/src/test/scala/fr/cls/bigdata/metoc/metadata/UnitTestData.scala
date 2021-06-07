package fr.cls.bigdata.metoc.metadata

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, DimensionRef, Dimensions, VariableRef}
import spray.json._

trait UnitTestData {

  import DefaultJsonProtocol._

  object Attribute {
    val name = "some attribute"
    val metadata: MetadataAttribute = MetadataAttribute(
      name,
      dataType = DataType.Int,
      values = Seq(1, 2, 3)
    )
    val json: JsObject = JsObject(
      "netcdfType" -> "int".toJson,
      "values" -> Seq(1, 2, 3).toJson
    )
  }

  object Longitude {
    val ref = DimensionRef("longitude")

    val metadata = DimensionMetadata(
      "longitude full name",
      DataType.Double,
      Set(MetadataAttribute("some attribute", DataType.Boolean, Seq(false)))
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "longitude full name".toJson,
      "netcdfType" -> "double".toJson,
      "attributes" -> JsObject(
        "some attribute" -> JsObject(
          "netcdfType" -> "boolean".toJson,
          "values" -> Seq(false).toJson
        )
      )
    )
  }

  object Latitude {
    val ref = DimensionRef("latitude")

    val metadata = DimensionMetadata(
      "latitude full name",
      DataType.Double,
      Set()
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "latitude full name".toJson,
      "netcdfType" -> "double".toJson,
      "attributes" -> JsObject()
    )
  }

  object Time {
    val ref = DimensionRef("time")

    val metadata = DimensionMetadata(
      "time full name",
      DataType.Double,
      Set()
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "time full name".toJson,
      "netcdfType" -> "double".toJson,
      "attributes" -> JsObject()
    )
  }

  object Depth {
    val ref = DimensionRef("depth")

    val metadata = DimensionMetadata(
      "depth full name",
      DataType.Double,
      Set()
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "depth full name".toJson,
      "netcdfType" -> "double".toJson,
      "attributes" -> JsObject()
    )
  }

  object Variable3D {
    val ref = VariableRef("var_3D")

    val metadata = VariableMetadata(
      "variable 3D",
      DataType.Long,
      Seq(DimensionRef("longitude"), DimensionRef("latitude"), DimensionRef("time")),
      Set(MetadataAttribute("some attribute", DataType.String, Seq("value1", "value2")))
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "variable 3D".toJson,
      "netcdfType" -> "long".toJson,
      "dimensions" -> Seq("longitude", "latitude", "time").toJson,
      "attributes" -> JsObject(
        "some attribute" -> JsObject(
          "netcdfType" -> "String".toJson,
          "values" -> Seq("value1", "value2").toJson
        )
      )
    )
  }

  object Variable4D {
    val ref = VariableRef("var_4D")

    val metadata = VariableMetadata(
      "variable 4D",
      DataType.Long,
      Seq(DimensionRef("longitude"), DimensionRef("latitude"), DimensionRef("time"), DimensionRef("depth")),
      Set(MetadataAttribute("some attribute", DataType.String, Seq("value1", "value2")))
    )

    val json: JsObject = JsObject(
      "netcdfName" -> "variable 4D".toJson,
      "netcdfType" -> "long".toJson,
      "dimensions" -> Seq("longitude", "latitude", "time", "depth").toJson,
      "attributes" -> JsObject(
        "some attribute" -> JsObject(
          "netcdfType" -> "String".toJson,
          "values" -> Seq("value1", "value2").toJson
        )
      )
    )
  }

  object Dataset {
    val metadata = DatasetMetadata(
      Map(
        Dimensions.longitude -> Longitude.metadata,
        Dimensions.latitude -> Latitude.metadata,
        Dimensions.time -> Time.metadata,
        Dimensions.depth -> Depth.metadata
      ),
      Map(
        Variable3D.ref -> Variable3D.metadata,
        Variable4D.ref -> Variable4D.metadata
      ),
      Set(Attribute.metadata)
    )
    val json: JsObject = JsObject(
      "dimensions" -> JsObject(
        "longitude" -> Longitude.json,
        "latitude" -> Latitude.json,
        "time" -> Time.json,
        "depth" -> Depth.json
      ),
      "variables" -> JsObject(
        "var_3D" -> Variable3D.json,
        "var_4D" -> Variable4D.json
      ),
      "attributes" -> JsObject(
        "some attribute" -> Attribute.json
      )
    )
  }

}
