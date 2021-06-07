package fr.cls.bigdata.metoc.metadata

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import spray.json._

private[metadata] object MetadataJsonFormat extends JsonFormat[DatasetMetadata] with DefaultJsonProtocol {
  @throws[DeserializationException]
  override def read(json: JsValue): DatasetMetadata = {
    DatasetMetadata(
      dimensions = for ((ref, dimensionJson) <- (json / "dimensions").fields)
        yield DimensionRef(ref) -> readDimension(dimensionJson),
      variables = for ((ref, variableJson) <- (json / "variables").fields)
        yield VariableRef(ref) -> readVariable(variableJson),
      attributes = attributesFromJson(json / "attributes")
    )
  }

  override def write(dataset: DatasetMetadata): JsObject = {
    JsObject(
      "dimensions" -> JsObject(
        for ((ref, dimensionMetadata) <- dataset.dimensions)
          yield ref.name -> write(dimensionMetadata)
      ),
      "variables" -> JsObject(
        for ((ref, variableMetadata) <- dataset.variables)
          yield ref.name -> write(variableMetadata)
      ),
      "attributes" -> write(dataset.attributes)
    )
  }

  def readDimension(json: JsValue): DimensionMetadata = {
    DimensionMetadata(
      (json / "netcdfName").convertTo[String],
      dataTypeFromJson(json / "netcdfType"),
      attributesFromJson(json / "attributes")
    )
  }

  def write(dimension: DimensionMetadata): JsObject = {
    JsObject(
      "netcdfName" -> dimension.shortName.toJson,
      "netcdfType" -> write(dimension.dataType),
      "attributes" -> write(dimension.attributes)
    )
  }

  def readVariable(json: JsValue): VariableMetadata = {
    VariableMetadata(
      (json / "netcdfName").convertTo[String],
      dataTypeFromJson(json / "netcdfType"),
      (json / "dimensions").convertTo[Seq[String]].map(DimensionRef),
      attributesFromJson(json / "attributes")
    )
  }

  def write(variable: VariableMetadata): JsObject = {
    JsObject(
      "netcdfName" -> variable.shortName.toJson,
      "netcdfType" -> write(variable.dataType),
      "dimensions" -> variable.dimensions.map(_.name).toJson,
      "attributes" -> write(variable.attributes)
    )
  }

  def attributesFromJson(json: JsValue): Set[MetadataAttribute] = {
    for ((name, attribute) <- json.fields.toSet) yield readAttribute(name, attribute)
  }

  def write(attributes: Set[MetadataAttribute]): JsObject = {
    JsObject(attributes.map(attribute => attribute.name -> write(attribute)).toMap)
  }

  def readAttribute(name: String, json: JsValue): MetadataAttribute = {
    dataTypeFromJson(json / "netcdfType") match {
      case numericType: DataType.NumericType =>
        val values = (json / "values").convertTo[Seq[Double]]
        numericType match {
          case DataType.Byte => MetadataAttribute(name, DataType.Byte, values.map(DataType.Byte.of))
          case DataType.Short => MetadataAttribute(name, DataType.Short, values.map(DataType.Short.of))
          case DataType.Int => MetadataAttribute(name, DataType.Int, values.map(DataType.Int.of))
          case DataType.Long => MetadataAttribute(name, DataType.Long, values.map(DataType.Long.of))
          case DataType.Float => MetadataAttribute(name, DataType.Float, values.map(DataType.Float.of))
          case DataType.Double => MetadataAttribute(name, DataType.Double, values.map(DataType.Double.of))
        }
      case DataType.Boolean => MetadataAttribute(name, DataType.Boolean, (json / "values").convertTo[Seq[Boolean]])
      case DataType.String => MetadataAttribute(name, DataType.String, (json / "values").convertTo[Seq[String]])
    }
  }

  def write(attribute: MetadataAttribute): JsObject = {
    JsObject(
      "netcdfType" -> write(attribute.dataType),
      attribute.dataType match {
        case numericType: DataType.NumericType =>
          "values" -> attribute.values.map(numericType.toDouble).toJson
        case DataType.Boolean =>
          "values" -> attribute.values.map(_.asInstanceOf[Boolean]).toJson
        case DataType.String =>
          "values" -> attribute.values.map(_.asInstanceOf[String]).toJson
      }
    )
  }

  def write(dataType: DataType): JsString = JsString {
    dataType match {
      case DataType.Byte => "byte"
      case DataType.Short => "short"
      case DataType.Int => "int"
      case DataType.Long => "long"
      case DataType.Float => "float"
      case DataType.Double => "double"
      case DataType.Boolean => "boolean"
      case DataType.String => "String"
    }
  }

  def dataTypeFromJson(str: JsValue): DataType = {
    str.convertTo[String] match {
      case "byte" => DataType.Byte
      case "short" => DataType.Short
      case "int" => DataType.Int
      case "long" => DataType.Long
      case "float" => DataType.Float
      case "double" => DataType.Double
      case "boolean" => DataType.Boolean
      case "String" => DataType.String
      case _ => throw new MetocReaderException(s"String $str cannot be converted to DataType")
    }
  }

  private implicit class RichJsObject(json: JsValue) {
    @throws[DeserializationException]
    def /(field: String): JsValue = {
      json.asJsObject.fields.getOrElse(field, throw DeserializationException(s"json object has no field $field"))
    }

    @throws[DeserializationException]
    def fields: Map[String, JsValue] = json.asJsObject.fields
  }

}
