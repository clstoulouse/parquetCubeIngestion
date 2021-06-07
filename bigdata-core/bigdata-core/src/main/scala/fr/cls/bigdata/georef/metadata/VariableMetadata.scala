package fr.cls.bigdata.georef.metadata

import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}

final case class VariableMetadata(shortName: String,
                                  dataType: DataType,
                                  dimensions: Seq[DimensionRef],
                                  attributes: Set[MetadataAttribute]) {
  def find(attributeName: String): Option[MetadataAttribute] = attributes.find(_.name == attributeName)
  def findAttrSingleValue[T](attributeName:String): Option[T] = find(attributeName).flatMap(_.singleValue)
  def withAttribute(name: String, dataType: DataType, values: Any*): VariableMetadata = {
    copy(attributes = attributes + MetadataAttribute(name, dataType, values))
  }
  def withAttribute(metadataAttribute: MetadataAttribute): VariableMetadata = {
    copy(attributes = attributes + metadataAttribute)
  }
}

object VariableMetadata {
  def apply(shortName: String, dataType: DataType, dimensions: Seq[DimensionRef]): VariableMetadata = {
    new VariableMetadata(shortName, dataType, dimensions, Set())
  }

  def create(ref: VariableRef, dataType: DataType, dimensions: Seq[DimensionRef], fillValue: Any): VariableMetadata = {
    VariableMetadata(ref.name, dataType, dimensions)
      .withAttribute(MetadataAttribute.standardName(ref.name))
      .withAttribute(MetadataAttribute.fillValueAttribute(dataType, fillValue))
  }
}