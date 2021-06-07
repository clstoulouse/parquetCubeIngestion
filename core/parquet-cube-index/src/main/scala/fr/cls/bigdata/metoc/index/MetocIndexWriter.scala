package fr.cls.bigdata.metoc.index

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.concurrent.DistributedLockingService
import fr.cls.bigdata.hadoop.config.DistributedLockingConfig
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.service.{MetocDatasetAccess, MetocWriter}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.hadoop.conf.Configuration

class MetocIndexWriter(service: MetocIndexService, hadoopConfiguration: Configuration) extends MetocWriter[Any] with LazyLogging {

  @throws[MetocWriterException]
  override def write(datasetSettings: DatasetSettings, dataset: MetocDatasetAccess[Any]): Unit = {
    val indexPath = datasetSettings.indexFolder
    logger.debug(s"creating/updating index for dataset '${datasetSettings.name}' at '$indexPath'...")

    val longitudeFile = MetocIndex.longitudeFile(indexPath)
    val latitudeFile = MetocIndex.latitudeFile(indexPath)
    val timeFile = MetocIndex.timeFile(indexPath)
    val depthFile = MetocIndex.depthFile(indexPath)

    val grid = dataset.grid

    service.writeMutableDimensionIndex(timeFile, grid.time)
    service.writeImmutableDimensionIndex(longitudeFile, grid.longitude)
    service.writeImmutableDimensionIndex(latitudeFile, grid.latitude)
    if (grid.depth.nonEmpty) {
      service.writeImmutableDimensionIndex(depthFile, grid.depth)
    }

    logger.debug(s"index created/updated for dataset '${datasetSettings.name}' at '$indexPath'")
  }
}

object MetocIndexWriter {
  def apply(lockingConfig: DistributedLockingConfig, hadoopConfig: Configuration): MetocIndexWriter = {
    val lockingService = DistributedLockingService(lockingConfig)
    val indexService = new MetocIndexService(lockingService)
    new MetocIndexWriter(indexService, hadoopConfig)
  }
}
