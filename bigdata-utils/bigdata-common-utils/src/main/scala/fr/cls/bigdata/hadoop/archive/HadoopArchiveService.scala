package fr.cls.bigdata.hadoop.archive

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.HadoopIOUtils
import fr.cls.bigdata.hadoop.config.ArchiveConfiguration
import fr.cls.bigdata.hadoop.model.PathWithFileSystem

/**
  * @define specsDescription <ul>
  *                          <li>If instructed to do so, it will copy the file to the success/failure archive folder</li>
  *                          <li>The archived file will have the target relative path in the archive folder, unless such file already exists.</li>
  *                          <li>The only case when the statement above does not hold is when a file with the same name and location exists in the target archive folder,
  *                          in which case an increment will be added as suffix to the file name during the copy</li>
  *                          <li>If instructed to do so, it will delete the original file after it was eventually copied to the proper archive location.</li>
  *                          </ul>
  */
trait HadoopArchiveService {

  /**
    * Given a file and a target relative path, this method will apply the <b>success</b> archiving strategy on it:
    * $specsDescription
    *
    * @param file File to apply the archiving strategy on.
    * @param relativePath The targeted relative path of the archived file in the archive folder
    * @throws IOException in case an error occurs while copying or deleting the file.
    */
  @throws[IOException]
  def onSuccess(file: PathWithFileSystem, relativePath: String): Unit

  /**
    * Given a file and a target relative path, this method will apply the <b>failure</b> archiving strategy on it:
    * $specsDescription
    *
    * @param file File to apply the archiving strategy on.
    * @param relativePath The targeted relative path of the archived file in the archive folder
    * @throws IOException in case an error occurs while copying or deleting the file.
    */
  @throws[IOException]
  def onFailure(file: PathWithFileSystem, relativePath: String): Unit
}

object HadoopArchiveService {
  def apply(onSuccess: ArchiveConfiguration, onFailure: ArchiveConfiguration): HadoopArchiveService = {
    val onSuccessStrategy = HadoopArchiveStrategy(onSuccess)
    val onFailureStrategy = HadoopArchiveStrategy(onFailure)
    new HadoopArchiveServiceImpl(HadoopIOUtils, onSuccessStrategy, onFailureStrategy)
  }

  private class HadoopArchiveServiceImpl(hadoopIo: HadoopIOUtils,
                                         onSuccessStrategy: HadoopArchiveStrategy,
                                         onFailureStrategy: HadoopArchiveStrategy) extends HadoopArchiveService with LazyLogging {

    @throws[IOException]
    override def onSuccess(file: PathWithFileSystem, targetRelativePath: String): Unit = {
      onSuccessStrategy(file: PathWithFileSystem, targetRelativePath: String)
    }

    @throws[IOException]
    override def onFailure(file: PathWithFileSystem, targetRelativePath: String): Unit = {
      onFailureStrategy(file: PathWithFileSystem, targetRelativePath: String)
    }
  }

}
