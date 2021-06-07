package fr.cls.bigdata.hadoop

import java.io.IOException

import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs.Path

class HadoopIOUtils(io: HadoopIO) {

  /**
    * Given a target path, this method will generate a unique path by concatenating the path with an increment suffix.
    * such that the resulting path will not correspond to any existing file or directory.
    *
    * @param parentFolder Target folder with its fileSystem.
    * @param name Target file name
    * @throws IOException If an error occurs while trying to access the target fileSystem.
    * @return The resulting path with its file system.
    */
  @throws[IOException]
  def generateUniquePath(parentFolder: PathWithFileSystem, name: String): PathWithFileSystem = {
    val target = parentFolder.child(name)
    if (!io.exists(target)) target else {
      Iterator.iterate(1)(i => i + 1)
        .map(i => target.map(path => new Path(s"$path.$i")))
        .dropWhile(io.exists)
        .next
    }
  }

  @throws[IOException]
  def manageTemporaryFolder(parentFolder: PathWithFileSystem, name: String, createFolder: Boolean): Resource[PathWithFileSystem] = {
    val temporaryFolder = generateUniquePath(parentFolder, name)

    if (createFolder) {
      io.createFolder(temporaryFolder)
    } else if (io.exists(temporaryFolder)) {
      throw new IOException(s"the temporary folder ${temporaryFolder.path} already exists")
    }

    new Resource[PathWithFileSystem] {
      def close(): Unit = {
        if (createFolder) io.deleteFolder(temporaryFolder)
        else if (io.exists(temporaryFolder)) io.deleteFolder(temporaryFolder)
      }

      def get: PathWithFileSystem = temporaryFolder
    }
  }

  @throws[IOException]
  def manageTemporaryFile(parentFolder: PathWithFileSystem, name: String): Resource[PathWithFileSystem] = {
    val temporaryFile = generateUniquePath(parentFolder, name)
    new Resource[PathWithFileSystem] {
      def close(): Unit = if (io.exists(temporaryFile)) io.deleteFile(temporaryFile)
      def get: PathWithFileSystem = temporaryFile
    }
  }
}

object HadoopIOUtils extends HadoopIOUtils(HadoopIO)
