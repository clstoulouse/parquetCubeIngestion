package fr.cls.bigdata.hadoop.model

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

case class AbsoluteAndRelativePath(path: Path, fileSystem: FileSystem, relativePath: String) {
  val name: String = path.getName
  def baseName: String = FilenameUtils.getBaseName(name)
  def extension: String = FilenameUtils.getExtension(name)
  val absolutePath = PathWithFileSystem(path, fileSystem)
  override def toString: String = absolutePath.toString
}

object AbsoluteAndRelativePath {
  def apply(pathWithFs: PathWithFileSystem, relativePath: String): AbsoluteAndRelativePath = {
    AbsoluteAndRelativePath(pathWithFs.path, pathWithFs.fileSystem, relativePath)
  }

  def apply(folder: PathWithFileSystem, absolutePath: Path): AbsoluteAndRelativePath = {
    val relativePath = StringUtils.stripStart(StringUtils.removeStart(absolutePath.toString, folder.toString), Path.SEPARATOR)
    AbsoluteAndRelativePath(absolutePath, folder.fileSystem, relativePath)
  }
}
