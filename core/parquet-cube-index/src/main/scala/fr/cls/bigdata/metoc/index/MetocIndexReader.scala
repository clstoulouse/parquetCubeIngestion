package fr.cls.bigdata.metoc.index

import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.hadoop.concurrent.DistributedLockingService
import fr.cls.bigdata.hadoop.config.DistributedLockingConfig
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.impl.BinarySearchGridService
import fr.cls.bigdata.metoc.model.Grid
import fr.cls.bigdata.metoc.settings.DatasetSettings
import fr.cls.bigdata.metoc.service.{GridServiceFactory, MetocDimensionReader, MetocGridService}
import org.apache.hadoop.conf.Configuration

import scala.collection.SortedSet

class MetocIndexReader(indexService: MetocIndexService, hadoopConfiguration: Configuration)
  extends MetocDimensionReader with GridServiceFactory {
  @throws[MetocReaderException]
  override def create(datasetSettings: DatasetSettings): MetocGridService = {
    val longitude = readLongitude(datasetSettings)
    val latitude = readLatitude(datasetSettings)
    val time = readTime(datasetSettings)
    val depth = readDepth(datasetSettings).getOrElse(SortedSet.empty[Double])
    new BinarySearchGridService(Grid(longitude, latitude, time, depth))
  }

  @throws[MetocReaderException]
  def readLongitude(datasetSettings: DatasetSettings): SortedSet[Double] = {
    val longitudeFile = MetocIndex.longitudeFile(datasetSettings.indexFolder)
    indexService.readDoubleValues(longitudeFile)
  }

  @throws[MetocReaderException]
  def readLatitude(datasetSettings: DatasetSettings): SortedSet[Double] = {
    val latitudeFile = MetocIndex.latitudeFile(datasetSettings.indexFolder)
    indexService.readDoubleValues(latitudeFile)
  }

  @throws[MetocReaderException]
  def readTime(datasetSettings: DatasetSettings): SortedSet[Long] = {
    val timeFile = MetocIndex.timeFile(datasetSettings.indexFolder)
    indexService.readLongValues(timeFile)
  }

  @throws[MetocReaderException]
  def readDepth(datasetSettings: DatasetSettings): Option[SortedSet[Double]] = {
    val depthFile = MetocIndex.depthFile(datasetSettings.indexFolder)

    if (HadoopIO.exists(depthFile)) {
      Some(indexService.readDoubleValues(depthFile))
    } else None
  }
}

object MetocIndexReader {
  def apply(lockingConfig: DistributedLockingConfig, hadoopConfig: Configuration): MetocIndexReader = {
    val lockingService = DistributedLockingService(lockingConfig)
    val indexService = new MetocIndexService(lockingService)
    new MetocIndexReader(indexService, hadoopConfig)
  }
}
