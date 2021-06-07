package fr.cls.bigdata.metoc.metadata.diff

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}
import fr.cls.bigdata.metoc.metadata.UnitTestData

object UnitTestDiffData extends UnitTestData {

  abstract class ChangeCase {
    def metadata1: DatasetMetadata
    def metadata2: DatasetMetadata
    def changes: Set[MetadataChange]
  }

  trait NonBreakingChangeCase {
    this: ChangeCase =>

    def commonRoot: DatasetMetadata
  }

  trait AttributeChangeCase {
    this: ChangeCase with NonBreakingChangeCase =>

    def attributes1: Set[MetadataAttribute]
    def attributes2: Set[MetadataAttribute]
    def attributesChanges: Set[MetadataAttributeChange]

    def applyAttributes(metadata: DatasetMetadata, attributes: Set[MetadataAttribute]): DatasetMetadata
    def applyAttributesChanges(changes: Set[MetadataAttributeChange]): Set[MetadataChange]

    override def metadata1: DatasetMetadata = applyAttributes(Dataset.metadata, attributes1)
    override def metadata2: DatasetMetadata = applyAttributes(Dataset.metadata, attributes2)
    override def changes: Set[MetadataChange] = applyAttributesChanges(attributesChanges)
    override def commonRoot: DatasetMetadata = applyAttributes(metadata1, attributes1.intersect(attributes2))
  }


  trait DatasetAttributeChangeCase {
    this: AttributeChangeCase with NonBreakingChangeCase =>

    override def applyAttributes(metadata: DatasetMetadata, attributes: Set[MetadataAttribute]): DatasetMetadata = metadata.copy(attributes = attributes)
    override def applyAttributesChanges(changes: Set[MetadataAttributeChange]): Set[MetadataChange] = Set(MetadataChange.DatasetAttributesChanges(changes))

  }

  trait DimensionAttributeChangeCase {
    this: AttributeChangeCase with NonBreakingChangeCase =>

    override def applyAttributes(metadata: DatasetMetadata, attributes: Set[MetadataAttribute]): DatasetMetadata = {
      val modifiedDimension = Longitude.metadata.copy(attributes = attributes)

      metadata.copy(dimensions = metadata.dimensions.updated(Longitude.ref, modifiedDimension))
    }

    override def applyAttributesChanges(changes: Set[MetadataAttributeChange]): Set[MetadataChange] = Set(MetadataChange.DimensionAttributesChanges(Longitude.ref, changes))
  }

  trait VariableAttributeChangeCase {
    this: AttributeChangeCase with NonBreakingChangeCase =>

    override def applyAttributes(metadata: DatasetMetadata, attributes: Set[MetadataAttribute]): DatasetMetadata = {
      val modifiedVariable = Variable3D.metadata.copy(attributes = attributes)

      metadata.copy(variables = metadata.variables.updated(Variable3D.ref, modifiedVariable))
    }

    override def applyAttributesChanges(changes: Set[MetadataAttributeChange]): Set[MetadataChange] = Set(MetadataChange.VariableAttributesChanges(Variable3D.ref, changes))
  }

  trait OnlyLeftAttributeChangeCase {
    this: ChangeCase with AttributeChangeCase with NonBreakingChangeCase =>

    override def attributes1: Set[MetadataAttribute] = Set(Attribute.metadata)

    override def attributes2: Set[MetadataAttribute] = Set.empty

    override def attributesChanges: Set[MetadataAttributeChange] = Set(MetadataAttributeChange.OnlyOneSide(DiffSide.Left, Attribute.metadata))

    override def commonRoot: DatasetMetadata = metadata2

  }

  trait OnlyRightAttributeChangeCase {
    this: ChangeCase with AttributeChangeCase with NonBreakingChangeCase =>

    override def attributes1: Set[MetadataAttribute] = Set.empty

    override def attributes2: Set[MetadataAttribute] = Set(Attribute.metadata)

    override def attributesChanges: Set[MetadataAttributeChange] = Set(MetadataAttributeChange.OnlyOneSide(DiffSide.Right, Attribute.metadata))

    override def commonRoot: DatasetMetadata = metadata1

  }

  trait TypeChangeAttributeChangeCase {
    this: AttributeChangeCase with NonBreakingChangeCase =>

    override def attributes1: Set[MetadataAttribute] = Set(MetadataAttribute(Attribute.name, DataType.Int, Seq(1)))
    override def attributes2: Set[MetadataAttribute] = Set(MetadataAttribute(Attribute.name, DataType.Long, Seq(2L)))

    override def attributesChanges: Set[MetadataAttributeChange] = Set(MetadataAttributeChange.TypeChange(Attribute.metadata.name, DataType.Int, DataType.Long))

  }

  trait ValuesChangeAttributeChangeCase {
    this: AttributeChangeCase with NonBreakingChangeCase =>

    override def attributes1: Set[MetadataAttribute] = Set(MetadataAttribute(Attribute.name, DataType.Int, Seq(1)))
    override def attributes2: Set[MetadataAttribute] = Set(MetadataAttribute(Attribute.name, DataType.Int, Seq(2)))

    override def attributesChanges: Set[MetadataAttributeChange] = Set(MetadataAttributeChange.ValuesChange(Attribute.metadata.name, Seq(1), Seq(2)))

  }

  trait DimensionsChangeCase {
    this: ChangeCase =>

    def dimensions1: Map[DimensionRef, DimensionMetadata]
    def dimensions2: Map[DimensionRef, DimensionMetadata]

    override def metadata1: DatasetMetadata = Dataset.metadata.copy(dimensions = dimensions1)
    override def metadata2: DatasetMetadata = Dataset.metadata.copy(dimensions = dimensions2)

    override def changes: Set[MetadataChange]
  }

  trait SingleDimensionChangeCase {
    this: DimensionsChangeCase =>

    def dimension1: DimensionMetadata
    def dimension2: DimensionMetadata

    override def dimensions1: Map[DimensionRef, DimensionMetadata] = Map(Longitude.ref -> dimension1)
    override def dimensions2: Map[DimensionRef, DimensionMetadata] = Map(Longitude.ref -> dimension2)
  }

  trait VariablesChangeCase {
    this: ChangeCase =>

    def variables1: Map[VariableRef, VariableMetadata]
    def variables2: Map[VariableRef, VariableMetadata]

    override def metadata1: DatasetMetadata = Dataset.metadata.copy(variables = variables1)
    override def metadata2: DatasetMetadata = Dataset.metadata.copy(variables = variables2)

    override def changes: Set[MetadataChange]
  }

  trait SingleVariableChangeCase {
    this: VariablesChangeCase =>

    def variable1: VariableMetadata
    def variable2: VariableMetadata

    override def variables1: Map[VariableRef, VariableMetadata] = Map(Variable3D.ref -> variable1)
    override def variables2: Map[VariableRef, VariableMetadata] = Map(Variable3D.ref -> variable2)
  }


  object Cases {

    object DatasetAttributes {
      object OnlyLeftChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DatasetAttributeChangeCase with OnlyLeftAttributeChangeCase
      object OnlyRightChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DatasetAttributeChangeCase with OnlyRightAttributeChangeCase
      object TypeChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DatasetAttributeChangeCase with TypeChangeAttributeChangeCase
      object ValuesChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DatasetAttributeChangeCase with ValuesChangeAttributeChangeCase
    }

    object Dimension {

      object Attributes {

        object OnlyLeftChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DimensionAttributeChangeCase with OnlyLeftAttributeChangeCase

        object OnlyRightChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DimensionAttributeChangeCase with OnlyRightAttributeChangeCase

        object TypeChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DimensionAttributeChangeCase with TypeChangeAttributeChangeCase

        object ValuesChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with DimensionAttributeChangeCase with ValuesChangeAttributeChangeCase

      }

      object OnlyLeftChangeCase extends ChangeCase with DimensionsChangeCase {
        override def dimensions1: Map[DimensionRef, DimensionMetadata] = Map(Longitude.ref -> Longitude.metadata)

        override def dimensions2: Map[DimensionRef, DimensionMetadata] = Map.empty

        override def changes: Set[MetadataChange] = Set(MetadataChange.OneSideOnlyDimension(DiffSide.Left, Longitude.ref, Longitude.metadata))
      }

      object OnlyRightChangeCase extends ChangeCase with DimensionsChangeCase {
        override def dimensions1: Map[DimensionRef, DimensionMetadata] = Map.empty

        override def dimensions2: Map[DimensionRef, DimensionMetadata] = Map(Longitude.ref -> Longitude.metadata)

        override def changes: Set[MetadataChange] = Set(MetadataChange.OneSideOnlyDimension(DiffSide.Right, Longitude.ref, Longitude.metadata))
      }

      object TypeChangeCase extends ChangeCase with DimensionsChangeCase with SingleDimensionChangeCase {
        override def dimension1: DimensionMetadata = Longitude.metadata.copy(dataType = DataType.Int)

        override def dimension2: DimensionMetadata = Longitude.metadata.copy(dataType = DataType.Long)

        override def changes: Set[MetadataChange] = Set(MetadataChange.DimensionTypeChange(Longitude.ref, DataType.Int, DataType.Long))
      }

    }


    object Variables {

      object Attributes {
        object OnlyLeftChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with VariableAttributeChangeCase with OnlyLeftAttributeChangeCase

        object OnlyRightChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with VariableAttributeChangeCase with OnlyRightAttributeChangeCase

        object TypeChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with VariableAttributeChangeCase with TypeChangeAttributeChangeCase

        object ValuesChangeCase extends ChangeCase with NonBreakingChangeCase with AttributeChangeCase with VariableAttributeChangeCase with ValuesChangeAttributeChangeCase

      }

      object OnlyLeftChangeCase extends ChangeCase with VariablesChangeCase {
        override def variables1: Map[VariableRef, VariableMetadata] = Map(Variable3D.ref -> Variable3D.metadata)

        override def variables2: Map[VariableRef, VariableMetadata] = Map.empty

        override def changes: Set[MetadataChange] = Set(MetadataChange.OneSideOnlyVariable(DiffSide.Left, Variable3D.ref, Variable3D.metadata))
      }

      object OnlyRightChangeCase extends ChangeCase with VariablesChangeCase {
        override def variables1: Map[VariableRef, VariableMetadata] = Map.empty

        override def variables2: Map[VariableRef, VariableMetadata] = Map(Variable3D.ref -> Variable3D.metadata)

        override def changes: Set[MetadataChange] = Set(MetadataChange.OneSideOnlyVariable(DiffSide.Right, Variable3D.ref, Variable3D.metadata))
      }

      object TypeChangeCase extends ChangeCase with VariablesChangeCase with SingleVariableChangeCase {
        override def variable1: VariableMetadata = Variable3D.metadata.copy(dataType = DataType.Int)

        override def variable2: VariableMetadata = Variable3D.metadata.copy(dataType = DataType.Long)

        override def changes: Set[MetadataChange] = Set(MetadataChange.VariableTypeChange(Variable3D.ref, DataType.Int, DataType.Long))
      }

      object DependentDimensionsChangeCase extends ChangeCase with VariablesChangeCase with SingleVariableChangeCase {
        override def variable1: VariableMetadata = Variable3D.metadata.copy(dimensions = Seq.empty)

        override def variable2: VariableMetadata = Variable3D.metadata.copy(dimensions = Seq(Longitude.ref))

        override def changes: Set[MetadataChange] = Set(MetadataChange.DependentDimensionsChange(Variable3D.ref, Seq.empty, Seq(Longitude.ref)))
      }

    }


  }

}
