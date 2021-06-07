package fr.cls.bigdata.georef.metadata

import fr.cls.bigdata.georef.model.{DataType, Dimensions}

case class DimensionMetadata(shortName: String,
                             dataType: DataType,
                             attributes: Set[MetadataAttribute]) {
  def find(attributeName: String): Option[MetadataAttribute] = attributes.find(_.name == attributeName)
  def findSingleValue[T](attributeName:String): Option[T] = find(attributeName).flatMap(_.singleValue[T])
  def withAttribute(name: String, dataType: DataType, values: Any*): DimensionMetadata = {
    copy(attributes = attributes + MetadataAttribute(name, dataType, values))
  }
  def withAttribute(metadataAttribute: MetadataAttribute): DimensionMetadata = {
    copy(attributes = attributes + metadataAttribute)
  }
  def replaceAttribute(createIfNotFound : Boolean, name: String, dataType: DataType, values: Any*): DimensionMetadata = {
    val isAttributeExist = attributes.exists(p => p.name==name)
    if (isAttributeExist){
      copy(attributes = attributes.filterNot(p => p.equals(find(name).get)) + MetadataAttribute(name, dataType, values))
    } else if (createIfNotFound){
      copy(attributes = attributes + MetadataAttribute(name, dataType, values))
    }else {
      this
    }
  }
  def replaceAttribute(createIfNotFound : Boolean, metadataAttribute: MetadataAttribute): DimensionMetadata = {
    val isAttributeExist = attributes.exists(p => p.name==metadataAttribute.name)
    if (isAttributeExist){
      copy(attributes = attributes.filterNot(p => p.equals(find(metadataAttribute.name).get)) + metadataAttribute)
    } else if (createIfNotFound){
      copy(attributes = attributes + metadataAttribute)
    }else {
      this
    }
  }
}

object DimensionMetadata {
  def apply(shortName: String, dataType: DataType): DimensionMetadata = DimensionMetadata(shortName, dataType, Set())

  val longitude: DimensionMetadata = DimensionMetadata(Dimensions.longitude.name, DataType.Double)
    .withAttribute(MetadataAttribute.standardName(Dimensions.longitude.name))

  val latitude: DimensionMetadata = DimensionMetadata(Dimensions.latitude.name, DataType.Double)
    .withAttribute(MetadataAttribute.standardName(Dimensions.latitude.name))

  val time: DimensionMetadata = DimensionMetadata(Dimensions.time.name, DataType.Long)
    .withAttribute(MetadataAttribute.standardName(Dimensions.time.name))
    .withAttribute(MetadataAttribute.timestampUnitAttribute)
}
