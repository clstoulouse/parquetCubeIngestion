package fr.cls.bigdata.hadoop.model

import java.io.IOException

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class PathWithFileSystem(path: Path, fileSystem: FileSystem) {
  /**
    * Apply a transformation on the path without modifying the filesystem.
    *
    * @param f Transformation to apply to the current path to obtain the new one.
    * @return Copy of the current object with the new path.
    */
  def map(f: Path => Path): PathWithFileSystem = {
    this.copy(path = f(path))
  }

  def child(childName: String): PathWithFileSystem = map(new Path(_, childName))
  def parent: PathWithFileSystem = map(_.getParent)

  val name: String = path.getName
  def baseName: String = FilenameUtils.getBaseName(name)
  def extension: String = FilenameUtils.getExtension(name)

  override def toString: String = path.toString
}

object PathWithFileSystem {
  @throws[IOException]
  def apply(path: Path, hadoopConfiguration: Configuration): PathWithFileSystem = {
    new PathWithFileSystem(path, path.getFileSystem(hadoopConfiguration))
  }

  @throws[IOException]
  def apply(path: String, hadoopConfiguration: Configuration): PathWithFileSystem = {
    PathWithFileSystem(new Path(path), hadoopConfiguration)
  }
}
