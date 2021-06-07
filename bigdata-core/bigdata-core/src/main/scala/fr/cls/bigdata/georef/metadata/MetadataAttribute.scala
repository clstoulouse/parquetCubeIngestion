package fr.cls.bigdata.georef.metadata

import fr.cls.bigdata.georef.model.{DataStorage, DataType}

final case class MetadataAttribute(name: String, data: DataStorage) {
  def values: Seq[Any] = data
  def dataType: DataType = data.dataType
  def singleValue[T]: Option[T] = values.headOption.map(_.asInstanceOf[T])
}

object MetadataAttribute {
  def apply(name: String, dataType: DataType, values: Seq[Any]): MetadataAttribute = {
    MetadataAttribute(name, dataType.store(values))
  }

  def standardName(name: String) = MetadataAttribute(Constants.StandardNameAttribute, DataType.String, Seq(name))

  def fillValueAttribute(dataType: DataType, value: Any): MetadataAttribute = {
    MetadataAttribute(Constants.FillValueAttribute, dataType, Seq(value))
  }

  val timestampUnitAttribute: MetadataAttribute = {
    MetadataAttribute(Constants.UnitsAttribute, DataType.String, Seq("seconds since 1970-01-01 00:00:00"))
  }
}
