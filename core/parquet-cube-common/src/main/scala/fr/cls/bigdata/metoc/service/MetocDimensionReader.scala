package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.settings.DatasetSettings

import scala.collection.SortedSet

trait MetocDimensionReader {
  @throws[MetocReaderException]
  def readLongitude(datasetSettings: DatasetSettings): SortedSet[Double]

  @throws[MetocReaderException]
  def readLatitude(datasetSettings: DatasetSettings): SortedSet[Double]

  @throws[MetocReaderException]
  def readTime(datasetSettings: DatasetSettings): SortedSet[Long]

  @throws[MetocReaderException]
  def readDepth(datasetSettings: DatasetSettings): Option[SortedSet[Double]]
}
