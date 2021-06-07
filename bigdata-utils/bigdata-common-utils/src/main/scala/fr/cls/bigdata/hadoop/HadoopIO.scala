package fr.cls.bigdata.hadoop

import java.io.{FileNotFoundException, IOException}

import com.amazonaws.services.s3.model.AmazonS3Exception
import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.model.{AbsoluteAndRelativePath, PathWithFileSystem}
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs._

trait HadoopIO {
  /**
    * Returns the path where all symlinks are resolved.
    *
    * @param path the path to resolve
    * @throws IOException if an error occurs while accessing the file system.
    * @return the path where all symlinks are resolved.
    */
  @throws[IOException]
  def resolve(path: PathWithFileSystem): PathWithFileSystem

  /**
    * Initializes a folder.
    *
    * @param folder Path to the folder to initialize along with its file system.
    * @throws IOException If the folder does not exist or it exists but it is not a directory
    */
  @throws[IOException]
  def initFolder(folder: PathWithFileSystem): Unit

  /**
    * If the folder does not exist, it will attempt to create it.
    *
    * @param folder Path to the folder to initialize or create along with its file system.
    * @throws IOException If the folder exists and is not a directory or if the creation failed
    */
  def createFolderIfNotExist(folder: PathWithFileSystem): Unit

  /**
    * Assumes that the folder does not exist and tries to create it
    *
    * @param folder Path to the folder to create along with its file system.
    * @throws IOException If the folder exists or if the creation failed.
    */
  def createFolder(folder: PathWithFileSystem): Unit

  /**
    * List the files (excluding directories) in the input folder.
    *
    * @param folder Input folder along with its fileSystem.
    * @param recursive    If set to `true` the listing will be done recursively on the content of the input folder.
    * @throws IOException If an error occurs while listing the content of the directory.
    * @return Iterator of files.
    * @note the `next()` method of the iterator may raise an [[java.io.IOException]] in case of disconnection.
    */
  @throws[IOException]
  def listFilesInFolder(folder: PathWithFileSystem, recursive: Boolean): Iterator[AbsoluteAndRelativePath]

  /**
    * List the sub-folders in the input folder.
    *
    * @param folder Input folder along with its fileSystem.
    * @throws IOException If an error occurs while listing the content of the directory.
    * @return Iterator of folders
    * @note the `next()` method of the iterator may raise an [[java.io.IOException]] in case of disconnection.
    */
  @throws[IOException]
  def listSubFolders(folder: PathWithFileSystem): Iterator[AbsoluteAndRelativePath]

  /**
    * Copies or moves a file from a source to a destination (which might be on different file systems).
    *
    * @param source Source file along with its fileSystem.
    * @param destination Target file along with its fileSystem.
    * @param deleteSource     If set to `true` the file will be moved (the source will be deleted after the copy).
    * @throws IOException if an error occurs during the copy.
    */
  @throws[IOException]
  def copyOrMove(source: PathWithFileSystem, destination: PathWithFileSystem, deleteSource: Boolean): Unit

  /**
    * Replace a file from a source to a destination (which might be on different file systems).
    *
    * @param source Source file along with its fileSystem.
    * @param destination Target file along with its fileSystem.
    * @param deleteSource     If set to `true` the file will be moved (the source will be deleted after the copy).
    * @throws IOException if an error occurs during the copy.
    */
  @throws[IOException]
  def replace(source: PathWithFileSystem, destination: PathWithFileSystem, deleteSource: Boolean): Unit

  @throws[IOException]
  def tryMoveAtomically(source: AbsoluteAndRelativePath, destination: Path): Option[AbsoluteAndRelativePath]

  /**
    * Deletes a file.
    *
    * @param file File to delete along with its file system.
    * @throws IOException If any error occurs while deleting the file.
    */
  @throws[IOException]
  def deleteFile(file: PathWithFileSystem): Unit

  /**
    * Deletes a folder and all its content.
    *
    * @param folder Folder to delete along with its file system.
    * @throws IOException If any error occurs while deleting the folder.
    */
  @throws[IOException]
  def deleteFolder(folder: PathWithFileSystem): Unit

  /**
    * Retrieve the last modification timestamp of a file in ms.
    *
    * @param file The file along with its file system.
    * @throws IOException If any error occurs while retrieving the file status.
    */
  @throws[IOException]
  def lastModificationMillis(file: PathWithFileSystem): Long

  /**
    * @throws IOException if an error occurs while accessing the file system.
    * @return `true` if a file or directory exists in the path.
    */
  @throws[IOException]
  def exists(path: PathWithFileSystem): Boolean

  /**
    * @param file the file to create
    * @param overwrite overwrite the file if it exists
    * @throws IOException if an error occurs while creating the file.
    * @return an output stream
    */
  @throws[IOException]
  def createOutputStream(file: PathWithFileSystem, overwrite: Boolean): Resource[FSDataOutputStream]


  /**
    * @param file the file to open
    * @throws IOException if an error occurs while opening the file.
    * @return a stream of the file data
    */
  @throws[IOException]
  def openInputStream(file: PathWithFileSystem): Resource[FSDataInputStream]

  @throws[IOException]
  def fileStatus(file: PathWithFileSystem): FileStatus
}

object HadoopIO extends HadoopIO with LazyLogging {

  @throws[IOException]
  override def resolve(path: PathWithFileSystem): PathWithFileSystem = path.map(path.fileSystem.resolvePath)

  @throws[IOException]
  override def initFolder(folder: PathWithFileSystem): Unit = {
    val path = folder.path
    safeGetFileStatus(folder) match {
      case Some(fileStatus) if !fileStatus.isDirectory =>
        throw new IOException(s"The folder seems to exist but is not a directory: $path")
      case None =>
        throw new IOException(s"Folder does not exist: $path")
      case _ => ()
    }
  }

  @throws[IOException]
  override def createFolderIfNotExist(folder: PathWithFileSystem): Unit = {
    val path = folder.path
    safeGetFileStatus(folder) match {
      case Some(fileStatus) if !fileStatus.isDirectory =>
        throw new IOException(s"The folder seems to exist but is not a directory: $path")

      case None =>
        logger.debug(s"creating folder: $path")
        val result = folder.fileSystem.mkdirs(path)
        if (!result) {
          throw new IOException(s"Failed to create folder: $path")
        }

      case _ =>
        logger.debug(s"the folder already exists: $path")
    }
  }

  @throws[IOException]
  override def createFolder(folder: PathWithFileSystem): Unit = {
    val path = folder.path
    val fileSystem = folder.fileSystem

    safeGetFileStatus(folder) match {
      case Some(_) =>
        throw new IOException(s"The folder seems to exist: $path")

      case None =>
        logger.debug(s"creating folder: $path")
        val result = fileSystem.mkdirs(path)
        if (!result) {
          throw new IOException(s"Failed to create folder: $path")
        }
    }
  }

  @throws[IOException]
  private def safeGetFileStatus(folder: PathWithFileSystem): Option[FileStatus] = {
    try {
      Option(folder.fileSystem.getFileStatus(folder.path))
    } catch {
      case _: FileNotFoundException => None
    }
  }


  @throws[IOException]
  override def listFilesInFolder(folder: PathWithFileSystem, recursive: Boolean): Iterator[AbsoluteAndRelativePath] = {
    val hadoopIterator = folder.fileSystem.listFiles(folder.path, recursive)

    val iterator = new Iterator[FileStatus] {
      def hasNext: Boolean = hadoopIterator.hasNext
      def next(): FileStatus = hadoopIterator.next()
    }

    iterator.filter(_.isFile)
      .map(status => AbsoluteAndRelativePath(folder, status.getPath))
  }

  @throws[IOException]
  override def listSubFolders(folder: PathWithFileSystem): Iterator[AbsoluteAndRelativePath] = {
    val hadoopIterator = folder.fileSystem.listLocatedStatus(folder.path)

    val iterator = new Iterator[FileStatus] {
      def hasNext: Boolean = hadoopIterator.hasNext
      def next(): FileStatus = hadoopIterator.next()
    }

    iterator.filter(_.isDirectory)
      .map(status => AbsoluteAndRelativePath(folder, status.getPath))
  }

  @throws[IOException]
  override def copyOrMove(source: PathWithFileSystem, destination: PathWithFileSystem, deleteSource: Boolean): Unit = {
    try {
      HadoopIO.createFolderIfNotExist(destination.parent)
      logger.debug(s"${if (deleteSource) "moving" else "copying"} file: '${source.path}' -> '${destination.path}'...")
      try {
        val success = FileUtil.copy(source.fileSystem, source.path, destination.fileSystem, destination.path, deleteSource, source.fileSystem.getConf)
        if (success) {
          logger.info(s"file ${if (deleteSource) "moved" else "copied"}: '${source.path}' -> '${destination.path}'")
        } else {
          logger.error(s"failed to ${if (deleteSource) "move" else "copy"} file: '${source.path}' -> '${destination.path}'")
        }
      } catch {
        case t: AmazonS3Exception if "InvalidRange".equals(t.getErrorCode) =>
          logger.warn(s"It seems that the file '${source.path}' is an empty file on S3, creating an empty file on destination '${destination.path}': $t")
          destination.fileSystem.create(destination.path).close()
          logger.info(s"empty file created at '${destination.path}'")
          if (deleteSource) {
            logger.debug(s"deleting source file '${source.path}'...")
            source.fileSystem.delete(source.path, false)
            logger.info(s"source file deleted '${source.path}'")
          }
      }
    } catch {
      case t@(_: IOException | _: AmazonS3Exception) =>
        throw new IOException(s"Encountered an exception when tried to copy or move file: '${source.path}' -> '${destination.path}'", t)
    }
  }

  @throws[IOException]
  override def replace(source: PathWithFileSystem, destination: PathWithFileSystem, deleteSource: Boolean): Unit = {
    if (exists(destination)) {
      logger.info(s"file $destination already exists and will be replaced by $source")
      deleteFile(destination)
    }
    copyOrMove(source, destination, deleteSource)
  }

  @throws[IOException]
  override def tryMoveAtomically(sourceFile: AbsoluteAndRelativePath, destinationFolder: Path): Option[AbsoluteAndRelativePath] = {
    val fileSystem = sourceFile.fileSystem

    createFolderIfNotExist(PathWithFileSystem(destinationFolder, fileSystem))

    val destinationFile = new Path(destinationFolder, sourceFile.relativePath)
    try {
      val succeed = fileSystem.rename(sourceFile.path, destinationFile)
      if (succeed) {
        logger.debug(s"moved atomically: '${sourceFile.path}' -> '$destinationFile'")
        Some(sourceFile.copy(path = destinationFile))
      } else {
        logger.info(s"could not move atomically '${sourceFile.path}' -> '$destinationFile', it might have been picked up by another instance")
        None
      }
    } catch {
      case t: IOException =>
        throw new IOException(s"Encountered an exception when tried to move file atomically: '${sourceFile.path}' -> '$destinationFile'", t)
    }
  }

  @throws[IOException]
  override def deleteFile(file: PathWithFileSystem): Unit = {
    try {
      logger.debug(s"deleting file '${file.path}'...")
      val success = file.fileSystem.delete(file.path, false)
      if (success) {
        logger.debug(s"file deleted: '${file.path}'")
      } else {
        logger.warn(s"failed to delete file: '${file.path}'")
      }
    } catch {
      case t: IOException =>
        throw new IOException(s"Encountered an exception when tried to delete file: '${file.path}'", t)
    }
  }

  @throws[IOException]
  override def deleteFolder(folder: PathWithFileSystem): Unit = {
    try {
      logger.debug(s"deleting folder '${folder.path}'...")
      val success = folder.fileSystem.delete(folder.path, true)
      if (success) {
        logger.debug(s"folder deleted: '${folder.path}'")
      } else {
        logger.warn(s"failed to delete folder: '${folder.path}'")
      }
    } catch {
      case t: IOException =>
        throw new IOException(s"Encountered an exception when tried to delete folder: '${folder.path}'", t)
    }
  }

  def isFolder(file: PathWithFileSystem): Boolean = {
    file.fileSystem.isDirectory(file.path)
  }

  @throws[IOException]
  override def lastModificationMillis(file: PathWithFileSystem): Long = {
    val PathWithFileSystem(filePath, fileSystem) = file
    val fileStatus = fileSystem.getFileStatus(filePath)
    fileStatus.getModificationTime
  }

  @throws[IOException]
  override def exists(path: PathWithFileSystem): Boolean = {
    path.fileSystem.exists(path.path)
  }

  @throws[IOException]
  override def createOutputStream(file: PathWithFileSystem, overwrite: Boolean): Resource[FSDataOutputStream] = {
    Resource {
      file.fileSystem.create(file.path, overwrite)
    }
  }

  @throws[IOException]
  def openInputStream(file: PathWithFileSystem): Resource[FSDataInputStream] = {
    Resource {
      file.fileSystem.open(file.path)
    }
  }

  @throws[IOException]
  def fileStatus(file: PathWithFileSystem): FileStatus = {
    file.fileSystem.getFileStatus(file.path)
  }
}
