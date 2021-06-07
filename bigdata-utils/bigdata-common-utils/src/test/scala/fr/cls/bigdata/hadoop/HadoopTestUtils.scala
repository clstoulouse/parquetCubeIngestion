package fr.cls.bigdata.hadoop

import java.io.File
import java.util.UUID

import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{LocalFileSystem, Path, FileSystem => HadoopFS}

import scala.collection.mutable

trait HadoopTestUtils {
  final lazy val HadoopConfiguration = {
    val conf = new Configuration()
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    conf
  }
  final lazy val FileSystem = HadoopFS.get(HadoopConfiguration)

  def toHadoopPath(file: String): PathWithFileSystem = {
    PathWithFileSystem(new Path(s"file:///${FilenameUtils.separatorsToUnix(file)}"), FileSystem)
  }

  def toHadoopPath(file: File): PathWithFileSystem = {
    toHadoopPath(file.getPath)
  }

  def createTempDir(): PathWithFileSystem = {
    toHadoopPath(FileTestUtils.createTempDir())
  }

  def createTempFile(parent: PathWithFileSystem = createTempDir()): PathWithFileSystem = {
    val file = parent.child(UUID.randomUUID().toString)
    createEmptyFile(file)
    FileSystem.deleteOnExit(file.path)
    file
  }

  def createEmptyFile(file: PathWithFileSystem): Unit = {
    FileSystem.create(file.path, true).close()
  }

  def listDirectoryContent(dir: PathWithFileSystem, recursive: Boolean): Set[PathWithFileSystem] = {
    val inputFilesIterator = FileSystem.listFiles(dir.path, recursive)

    val paths = mutable.HashSet.empty[PathWithFileSystem]

    while (inputFilesIterator.hasNext) {
      val status = inputFilesIterator.next()
      if (status.isFile) {
        paths += PathWithFileSystem(status.getPath, FileSystem)
      }
    }

    paths.toSet
  }

  def simulateNoPermissionsOn(target: PathWithFileSystem): Unit = {
    FileSystem.setPermission(target.path, FsPermission.valueOf("----------"))
  }

  def simulateNoWritePermissionsOn(target: PathWithFileSystem): Unit = {
    if (FileSystem.isDirectory(target.path)) {
      FileSystem.setPermission(target.path, FsPermission.valueOf("-r-xr-xr-x"))
    } else {
      FileSystem.setPermission(target.path, FsPermission.valueOf("-r--r--r--"))
    }
  }
}

object HadoopTestUtils extends HadoopTestUtils
