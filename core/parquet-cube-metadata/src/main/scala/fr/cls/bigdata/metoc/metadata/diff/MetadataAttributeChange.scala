package fr.cls.bigdata.metoc.metadata.diff

import fr.cls.bigdata.georef.metadata.MetadataAttribute
import fr.cls.bigdata.georef.model.DataType


/**
  * Change on a list of metoc attributes.
  */
sealed trait MetadataAttributeChange {
  /**
    * @return Name of the attribute.
    */
  def name: String
}

object MetadataAttributeChange {

  /**
    * An attribute is defined only on one side of the diff.
    *
    * @param side      Side of the diff that defines the attribute.
    * @param attribute The attribute data.
    */
  final case class OnlyOneSide(side: DiffSide, attribute: MetadataAttribute) extends MetadataAttributeChange {
    override val name: String = attribute.name

    override def toString: String = {
      val strAction = side match {
        case DiffSide.Left => "(deleted)"
        case DiffSide.Right => "(created)"
      }
      s".attributes['$name']: $strAction"
    }
  }

  /**
    * The values are different on both sides of the diff.
    *
    * @param name        Name of the attribute.
    * @param leftValues  Attribute's value on the left side of the diff.
    * @param rightValues Attribute's value on the right side of the diff.
    */
  final case class ValuesChange(name: String,
                                leftValues: Seq[_],
                                rightValues: Seq[_]) extends MetadataAttributeChange {
    override def toString: String = {
      s".attributes['$name'].values: ${leftValues.mkString("[", ", ", "]")} -> ${rightValues.mkString("[", ", ", "]")}"
    }
  }

  /**
    * The data types are different on both side of the diff.
    *
    * @param name          Name of the attribute.
    * @param leftDataType  Attribute's data type on the left side of the diff.
    * @param rightDataType Attribute's data type on the right side of the diff.
    */
  final case class TypeChange(name: String,
                              leftDataType: DataType,
                              rightDataType: DataType) extends MetadataAttributeChange {
    override def toString: String = {
      s".attributes['$name'].dataType: $leftDataType -> $rightDataType"
    }
  }

}
