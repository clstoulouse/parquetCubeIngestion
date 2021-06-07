package fr.cls.bigdata.hadoop

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils

trait FileTestUtils {
  def createTempDir(): File = {
    val file = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    FileUtils.forceDeleteOnExit(file)
    file
  }
}

object FileTestUtils extends FileTestUtils
