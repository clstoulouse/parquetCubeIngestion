package fr.cls.bigdata.metoc.metadata.diff

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, DimensionRef, VariableRef}

/**
  * Represents a change detected when performing a diff between two [[fr.cls.bigdata.georef.metadata.DatasetMetadata]].
  */
sealed trait MetadataChange

object MetadataChange {

  /**
    * Marker trait for breaking changes.
    */
  sealed trait BreakingChange extends MetadataChange

  /**
    * Marker trait for non-breaking changes.
    */
  sealed trait NonBreakingChange extends MetadataChange {
    /**
      * Retrieves a common root the two versions of the metadata between which this change was detected.
      * <br>
      * Basically: if `change: NonBreakingChange` was produced when performing a diff between `metadata1: DatasetMetadata` and
      * `metadata2: DatasetMetadata`; then `change.inferCommonRoot(metadata1) == change.inferCommonRoot(metadata2)`
      *
      * @param datasetMetadata Metadata object on which this change was detected.
      * @return A metadata object from which the changes are removed (for example for an attribute change, it will remove the attribute from the input metadata).
      */
    def inferCommonRoot(datasetMetadata: DatasetMetadata): DatasetMetadata
  }

  /**
    * Base marker trait for changes related to a dimension of the dataset.
    */
  sealed trait DimensionRelatedChange extends MetadataChange {
    def dimensionRef: DimensionRef
  }

  /**
    * Base marker trait for changes related to a variable of the dataset.
    */
  sealed trait VariableRelatedChange extends MetadataChange {
    def variableRef: VariableRef
  }

  /**
    * A dimension exists only on one side of the diffed metadata objects.
    *
    * @param side              Side where the dimension exists.
    * @param dimensionRef      Identifier of the dimension.
    * @param dimensionMetadata Metadata of the dimension.
    */
  final case class OneSideOnlyDimension(side: DiffSide,
                                        dimensionRef: DimensionRef,
                                        dimensionMetadata: DimensionMetadata) extends MetadataChange with BreakingChange with DimensionRelatedChange {
    override def toString: String = {
      val strAction = side match {
        case DiffSide.Left => "(deleted)"
        case DiffSide.Right => "(created)"
      }
      s"dataset.dimensions['${dimensionRef.name}']: $strAction"
    }
  }

  /**
    * A variable exists only on one side of the diffed metadata objects.
    *
    * @param side             Side where the variable exists.
    * @param variableRef      Identifier of the variable.
    * @param variableMetadata Metadata of the variable.
    */
  final case class OneSideOnlyVariable(side: DiffSide,
                                       variableRef: VariableRef,
                                       variableMetadata: VariableMetadata) extends MetadataChange with BreakingChange with VariableRelatedChange {
    override def toString: String = {
      val strAction = side match {
        case DiffSide.Left => "(deleted)"
        case DiffSide.Right => "(created)"
      }
      s"dataset.variables['${variableRef.name}']: $strAction"
    }
  }

  /**
    * A dimension does not have the same data type on both sides of the diff.
    *
    * @param dimensionRef Identifier of the dimension.
    * @param leftType     The dimension's data type on the left side of the diff.
    * @param rightType    The dimension's data type on the right side of the diff.
    */
  final case class DimensionTypeChange(dimensionRef: DimensionRef,
                                       leftType: DataType,
                                       rightType: DataType) extends MetadataChange with BreakingChange with DimensionRelatedChange {
    override def toString: String = {
      s"dataset.dimensions['${dimensionRef.name}'].dataType: $leftType -> $rightType"
    }
  }

  /**
    * A variable does not have the same data type on both sides of the diff.
    *
    * @param variableRef Identifier of the variable.
    * @param leftType    The variable's data type on the left side of the diff.
    * @param rightType   The variable's data type on the right side of the diff.
    */
  final case class VariableTypeChange(variableRef: VariableRef,
                                      leftType: DataType,
                                      rightType: DataType) extends MetadataChange with BreakingChange with VariableRelatedChange {
    override def toString: String = {
      s"dataset.variables['${variableRef.name}'].dataType: $leftType -> $rightType"
    }
  }

  /**
    * A variable depends on a different sequence of dimensions on both sides of the diff.
    *
    * @param variableRef        Identifier of the variable.
    * @param leftDimensionRefs  List of dimensions on which the variable depends on the left side of the diff.
    * @param rightDimensionRefs List of dimensions on which the variable depends on the right side of the diff.
    */
  final case class DependentDimensionsChange(variableRef: VariableRef,
                                             leftDimensionRefs: Seq[DimensionRef],
                                             rightDimensionRefs: Seq[DimensionRef]) extends MetadataChange with BreakingChange with VariableRelatedChange {
    override def toString: String = {
      s"dataset.variables['${variableRef.name}'].dimensions: ${leftDimensionRefs.map(_.name)} -> ${rightDimensionRefs.map(_.name)}"
    }
  }

  /**
    * The dataset's attributes are not the same on both sides of the diff.
    *
    * @param changes List of differences detected on the dataset's attributes.
    */
  final case class DatasetAttributesChanges(changes: Set[MetadataAttributeChange]) extends MetadataChange with NonBreakingChange {
    /**
      * removes the dataset attribute.
      */
    override def inferCommonRoot(datasetMetadata: DatasetMetadata): DatasetMetadata = {
      datasetMetadata.copy(attributes = revertAttributesChanges(datasetMetadata.attributes, changes))
    }

    override def toString: String = {
      s"dataset.attributes:" + changes.mkString("\n  - ", "\n  - ", "")
    }
  }

  /**
    * A dimension's attributes are not the same on both sides of the diff.
    *
    * @param changes List of differences detected on the dimensions's attributes.
    */
  final case class DimensionAttributesChanges(dimensionRef: DimensionRef,
                                              changes: Set[MetadataAttributeChange]) extends MetadataChange with NonBreakingChange with DimensionRelatedChange {
    /**
      * removes the dataset attribute.
      */
    override def inferCommonRoot(datasetMetadata: DatasetMetadata): DatasetMetadata = {
      val dimension = datasetMetadata.dimensions(dimensionRef)
      val newVariable = dimension.copy(attributes = revertAttributesChanges(dimension.attributes, changes))

      datasetMetadata.copy(dimensions = datasetMetadata.dimensions.updated(dimensionRef, newVariable))
    }

    override def toString: String = {
      s"dataset.dimensions[${dimensionRef.name}]:" + changes.mkString("\n  - ", "\n  - ", "")
    }
  }

  /**
    * A variable's attributes are not the same on both sides of the diff.
    *
    * @param changes List of differences detected on the variable's attributes.
    */
  final case class VariableAttributesChanges(variableRef: VariableRef,
                                             changes: Set[MetadataAttributeChange]) extends MetadataChange with NonBreakingChange with VariableRelatedChange {
    /**
      * removes the dataset attribute.
      */
    override def inferCommonRoot(datasetMetadata: DatasetMetadata): DatasetMetadata = {
      val variable = datasetMetadata.variables(variableRef)
      val newVariable = variable.copy(attributes = revertAttributesChanges(variable.attributes, changes))

      datasetMetadata.copy(variables = datasetMetadata.variables.updated(variableRef, newVariable))
    }

    override def toString: String = {
      s"dataset.variables[${variableRef.name}]:" + changes.mkString("\n  - ", "\n  - ", "")
    }
  }



  private def revertAttributesChanges(attributes: Set[MetadataAttribute], changes: Set[MetadataAttributeChange]): Set[MetadataAttribute] = {
    val changedAttributes = changes.map(_.name)

    attributes.filter(x => !changedAttributes.contains(x.name))
  }
}
