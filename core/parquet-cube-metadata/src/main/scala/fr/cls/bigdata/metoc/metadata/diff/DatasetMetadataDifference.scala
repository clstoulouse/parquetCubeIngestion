package fr.cls.bigdata.metoc.metadata.diff

/**
  * Wraps the changes detected when comparing two [[fr.cls.bigdata.georef.metadata.DatasetMetadata]].
  *
  * @param breakingChanges    List of breaking changes.
  * @param nonBreakingChanges List of non-breaking changes.
  */
final case class DatasetMetadataDifference(breakingChanges: Set[MetadataChange.BreakingChange],
                                           nonBreakingChanges: Set[MetadataChange.NonBreakingChange])

object DatasetMetadataDifference {
  def apply(changes: Set[MetadataChange]): DatasetMetadataDifference = {
    changes.foldLeft(DatasetMetadataDifference(Set.empty, Set.empty)) {
      case (DatasetMetadataDifference(breakingChanges, nonBreakingChanges), change: MetadataChange.BreakingChange) =>
        DatasetMetadataDifference(breakingChanges + change, nonBreakingChanges)

      case (DatasetMetadataDifference(breakingChanges, nonBreakingChanges), change: MetadataChange.NonBreakingChange) =>
        DatasetMetadataDifference(breakingChanges, nonBreakingChanges + change)
    }
  }
}