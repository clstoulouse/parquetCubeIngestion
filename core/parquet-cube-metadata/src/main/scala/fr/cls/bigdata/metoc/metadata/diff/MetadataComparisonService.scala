package fr.cls.bigdata.metoc.metadata.diff

import fr.cls.bigdata.georef.metadata.{DatasetMetadata, DimensionMetadata, MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DimensionRef, VariableRef}

trait MetadataComparisonService {
  /**
    * Compares two objects of type [[fr.cls.bigdata.georef.metadata.DatasetMetadata]].
    *
    * @param metadata1 Left side of the comparison.
    * @param metadata2 Right side of the comparison.
    * @return Changes between the two objects wrapped in [[fr.cls.bigdata.metoc.metadata.diff.DatasetMetadataDifference]].
    */
  def compare(metadata1: DatasetMetadata, metadata2: DatasetMetadata): DatasetMetadataDifference

  /**
    * Modifies a [[fr.cls.bigdata.georef.metadata.DatasetMetadata]] by removing all elements on which a non-breaking change was detected.
    *
    * @param metadata           The source [[fr.cls.bigdata.georef.metadata.DatasetMetadata]].
    * @param nonBreakingChanges List of detected non breaking changes.
    * @return The modified [[fr.cls.bigdata.georef.metadata.DatasetMetadata]].
    */
  def inferCommonRoot(metadata: DatasetMetadata, nonBreakingChanges: Set[MetadataChange.NonBreakingChange]): DatasetMetadata
}

object MetadataComparisonService extends MetadataComparisonService {

  override def inferCommonRoot(metadata: DatasetMetadata, nonBreakingChanges: Set[MetadataChange.NonBreakingChange]): DatasetMetadata = {
    nonBreakingChanges.foldLeft(metadata) {
      case (acc, change) => change.inferCommonRoot(acc)
    }
  }

  override def compare(metadata1: DatasetMetadata, metadata2: DatasetMetadata): DatasetMetadataDifference = {
    val builder = Set.newBuilder[MetadataChange]

    builder ++= diffDimensions(metadata1.dimensions, metadata2.dimensions)
    builder ++= diffVariables(metadata1.variables, metadata2.variables)

    val attributesDiffs = diffAttributes(metadata1.attributes, metadata2.attributes)
    if (attributesDiffs.nonEmpty) {
      builder += MetadataChange.DatasetAttributesChanges(attributesDiffs)
    }

    DatasetMetadataDifference(builder.result())
  }

  private[diff] def diffDimensions(dimensions1: Map[DimensionRef, DimensionMetadata], dimensions2: Map[DimensionRef, DimensionMetadata]): Set[MetadataChange.DimensionRelatedChange] = {
    val builder = Set.newBuilder[MetadataChange.DimensionRelatedChange]

    val (commonDimensions, onlyLeftDimensions, onlyRightDimensions) = extractKeySets(dimensions1, dimensions2)

    for ((dimensionRef, dimension) <- onlyLeftDimensions) {
      builder += MetadataChange.OneSideOnlyDimension(DiffSide.Left, dimensionRef, dimension)
    }
    for ((dimensionRef, dimension) <- onlyRightDimensions) {
      builder += MetadataChange.OneSideOnlyDimension(DiffSide.Right, dimensionRef, dimension)
    }
    for ((dimensionRef, (leftDimension, rightDimension)) <- commonDimensions) {
      if (leftDimension.dataType != rightDimension.dataType) {
        builder += MetadataChange.DimensionTypeChange(dimensionRef, leftDimension.dataType, rightDimension.dataType)
      }

      val attributesDiffs = diffAttributes(leftDimension.attributes, rightDimension.attributes)

      if (attributesDiffs.nonEmpty) {
        builder += MetadataChange.DimensionAttributesChanges(dimensionRef, attributesDiffs)
      }
    }

    builder.result()
  }

  private[diff] def diffVariables(variables1: Map[VariableRef, VariableMetadata], variables2: Map[VariableRef, VariableMetadata]): Set[MetadataChange.VariableRelatedChange] = {
    val builder = Set.newBuilder[MetadataChange.VariableRelatedChange]

    val (commonVariables, onlyLeftVariables, onlyRightVariables) = extractKeySets(variables1, variables2)

    for ((variableRef, variable) <- onlyLeftVariables) {
      builder += MetadataChange.OneSideOnlyVariable(DiffSide.Left, variableRef, variable)
    }
    for ((variableRef, variable) <- onlyRightVariables) {
      builder += MetadataChange.OneSideOnlyVariable(DiffSide.Right, variableRef, variable)
    }
    for ((variableRef, (leftVariable, rightVariable)) <- commonVariables) {
      if (leftVariable.dataType != rightVariable.dataType) {
        builder += MetadataChange.VariableTypeChange(variableRef, leftVariable.dataType, rightVariable.dataType)
      }

      if (leftVariable.dimensions != rightVariable.dimensions) {
        builder += MetadataChange.DependentDimensionsChange(variableRef, leftVariable.dimensions, rightVariable.dimensions)
      }

      val attributesDiffs = diffAttributes(leftVariable.attributes, rightVariable.attributes)

      if (attributesDiffs.nonEmpty) {
        builder += MetadataChange.VariableAttributesChanges(variableRef, attributesDiffs)
      }
    }

    builder.result()
  }

  private[diff] def diffAttributes(attributes1: Set[MetadataAttribute], attributes2: Set[MetadataAttribute]): Set[MetadataAttributeChange] = {
    val builder = Set.newBuilder[MetadataAttributeChange]

    val map1: Map[String, MetadataAttribute] = attributes1.map(x => x.name -> x).toMap
    val map2: Map[String, MetadataAttribute] = attributes2.map(x => x.name -> x).toMap

    val (commonAttributes, onlyLeftAttributes, onlyRightAttributes) = extractKeySets(map1, map2)

    for ((_, attribute) <- onlyLeftAttributes) {
      builder += MetadataAttributeChange.OnlyOneSide(DiffSide.Left, attribute)
    }
    for ((_, attribute) <- onlyRightAttributes) {
      builder += MetadataAttributeChange.OnlyOneSide(DiffSide.Right, attribute)
    }

    for ((name, (leftAttribute, rightAttribute)) <- commonAttributes) {
      if (leftAttribute.dataType != rightAttribute.dataType) {
        builder += MetadataAttributeChange.TypeChange(name, leftAttribute.dataType, rightAttribute.dataType)
      } else if (leftAttribute.values != rightAttribute.values) {
        builder += MetadataAttributeChange.ValuesChange(name, leftAttribute.values, rightAttribute.values)
      }
    }

    builder.result()
  }

  private def extractKeySets[K, V](map1: Map[K, V], map2: Map[K, V]): (Map[K, (V, V)], Map[K, V], Map[K, V]) = {
    val commonKeys = map1.keySet.intersect(map2.keySet).map(key => key -> (map1(key), map2(key))).toMap

    val onlyLeftKeys = map1.keySet.diff(commonKeys.keySet).map(key => key -> map1(key)).toMap

    val onlyRightKeys = map2.keySet.diff(commonKeys.keySet).map(key => key -> map2(key)).toMap

    (commonKeys, onlyLeftKeys, onlyRightKeys)
  }
}
