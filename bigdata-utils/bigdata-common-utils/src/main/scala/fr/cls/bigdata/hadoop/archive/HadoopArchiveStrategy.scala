package fr.cls.bigdata.hadoop.archive

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.config.ArchiveConfiguration
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.hadoop.{HadoopIO, HadoopIOUtils}

private[archive] class HadoopArchiveStrategy(copyTo: Option[PathWithFileSystem], deleteInputFile: Boolean, io: HadoopIO) extends LazyLogging {
  private val ioUtils = new HadoopIOUtils(io)

  /**
    * applies the archive strategy to the file:
    * - if copyTo is defined, move the file to the copyTo folder
    * - if deleteInputFile is true delete the inputFile
    *
    * @param file The file to archive.
    * @param relativePath the targeted relative path of the archive file in the archive folder
    */
  def apply(file: PathWithFileSystem, relativePath: String): Unit = {
    copyTo match {
      case Some(targetFolder) =>
        val targetPathWithFs = ioUtils.generateUniquePath(targetFolder, relativePath)
        io.copyOrMove(file, targetPathWithFs, deleteInputFile)

      case None if deleteInputFile => io.deleteFile(file)

      case None => logger.debug(s"The input-file will stay in place: ${file.path}")
    }
  }
}

object HadoopArchiveStrategy {
  @throws[IOException]
  def apply(archiveConfig: ArchiveConfiguration): HadoopArchiveStrategy = {
    new HadoopArchiveStrategy(archiveConfig.copyTo, archiveConfig.removeInputFile, HadoopIO)
  }
}