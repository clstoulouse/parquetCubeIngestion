package fr.cls.bigdata.georef.metadata

import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}

final case class DatasetMetadata(dimensions: Map[DimensionRef, DimensionMetadata],
                                 variables: Map[VariableRef, VariableMetadata],
                                 attributes: Set[MetadataAttribute]) {
  def find(attributeName: String): Option[MetadataAttribute] = attributes.find(_.name == attributeName)
  def findAttrSingleValue(attributeName:String): Option[Any] = find(attributeName).flatMap(_.singleValue)
  def withAttribute(name: String, dataType: DataType, values: Any*): DatasetMetadata = {
    copy(attributes = attributes + MetadataAttribute(name, dataType, values))
  }
  def withAttribute(metadataAttribute: MetadataAttribute): DatasetMetadata = {
    copy(attributes = attributes + metadataAttribute)
  }
  def withDimension(ref: DimensionRef, metadata: DimensionMetadata): DatasetMetadata = {
    copy(dimensions = dimensions + (ref -> metadata))
  }
  def withVariable(ref: VariableRef, metadata: VariableMetadata): DatasetMetadata = {
    copy(variables = variables + (ref -> metadata))
  }
}

object DatasetMetadata {
  def apply(): DatasetMetadata = DatasetMetadata(Map(), Map(), Set())
}
