package fr.cls.bigdata.hadoop.concurrent

import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.hadoop.model.{AbsoluteAndRelativePath, PathWithFileSystem}
import org.apache.hadoop.fs.Path

/**
  * Service that can be used to ensure that multiple process/threads can pick up and work on the content of a folder without having
  * 2 process/threads picking up the same file at the same time.
  *
  * It does so by atomically moving (given the the underlying filesystem allows it) picked up files to an "in-progress" folder so they become "invisible"
  * to other processes.
  */
trait ConcurrentFilePicker {

  /**
    * Picks a random file in the input directory and move it atomically to an in-progress folder located directly under the input folder (it will be created if it does not exist) before returning.
    *
    * @param recursive If set to `true` the file will be picked up from the input folder or its sub-folders (excluding the inprogress folder). If set to `false` only the files located directly under the input folder can be picked up.
    * @param predicate Predicate to apply on the file before deciding to pick it up. If it returns `false` the file will be skipped and the method will attempt another one.
    * @throws IOException In case an error occurs.
    * @return The picked up file wrapped in a scala option. If the input folder does not contain any valid file to pick up, it returns `None`.
    * @note using this method ensures synchronization when multiple threads/processes try to pick up files concurrently, it will avoid having the same file processed by two threads/processes.
    */
  @throws[IOException]
  def pickOneFile(recursive: Boolean, predicate: AbsoluteAndRelativePath => Boolean): Option[AbsoluteAndRelativePath]

}

object ConcurrentFilePicker {
  /**
    * Constructs a new instance of [[fr.cls.bigdata.hadoop.concurrent.ConcurrentFilePicker]].
    *
    * @param inputFolder    Input folder where the files will be picked up, along with its filesystem.
    * @param inProgressFolder Sub-folder (relative to the input folder) where the picked up files will be moved to
    *                             protect them from being picked up by other threads/process.
    * @return
    */
  def apply(inputFolder: PathWithFileSystem, inProgressFolder: String): ConcurrentFilePicker = new ConcurrentFilePicker with LazyLogging {

    @throws[IOException]
    override def pickOneFile(recursive: Boolean, predicate: AbsoluteAndRelativePath => Boolean): Option[AbsoluteAndRelativePath] = {
      val inProgressFilePath = new Path(inputFolder.path, inProgressFolder)

      val filteredIterator = for {
        inputFile <- HadoopIO.listFilesInFolder(inputFolder, recursive)
        if !inputFile.path.toString.startsWith(inProgressFilePath.toString) && predicate(inputFile)
        movedInputFile <- HadoopIO.tryMoveAtomically(inputFile, inProgressFilePath)
      } yield movedInputFile

      if (filteredIterator.hasNext) {
        Some(filteredIterator.next())
      } else {
        None
      }
    }
  }
}


