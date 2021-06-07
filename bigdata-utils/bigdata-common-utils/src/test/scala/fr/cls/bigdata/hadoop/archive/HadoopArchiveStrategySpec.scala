package fr.cls.bigdata.hadoop.archive

import java.io.IOException

import fr.cls.bigdata.hadoop.{HadoopIO, HadoopTestUtils}
import fr.cls.bigdata.hadoop.config.ArchiveConfiguration
import fr.cls.bigdata.hadoop.model.AbsoluteAndRelativePath
import org.scalatest.{FunSpec, Matchers}

class HadoopArchiveStrategySpec extends FunSpec with Matchers with HadoopTestUtils {

  describe("apply()") {
    it("should create the archive folder if it does not exist") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempDir().child("non-existent")
      val archiveConf = ArchiveConfiguration(Some(archiveDir), removeInputFile = false)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)
      archiveStrategy(inputFile.absolutePath, inputFile.relativePath)

      HadoopIO.exists(archiveDir) shouldBe true
      HadoopIO.isFolder(archiveDir) shouldBe true
    }

    it("should throw an IOException is the archive path exists and is file") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempFile()
      val archiveConf = ArchiveConfiguration(copyTo = Some(archiveDir), removeInputFile = false)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)

      a[IOException] should be thrownBy archiveStrategy(inputFile.absolutePath, inputFile.relativePath)
    }

    it("should throw an IOException is archive folder cannot be created") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempDir().child("non-existent")
      simulateNoWritePermissionsOn(archiveDir.parent)
      val archiveConf = ArchiveConfiguration(copyTo = Some(archiveDir), removeInputFile = false)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)

      a[IOException] should be thrownBy archiveStrategy(inputFile.absolutePath, inputFile.relativePath)
    }

    it("should move the input file to the archive folder if instructed to do so") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempDir()
      val archiveConf = ArchiveConfiguration(copyTo = Some(archiveDir), removeInputFile = true)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)
      archiveStrategy(inputFile.absolutePath, inputFile.relativePath)

      listDirectoryContent(inputFolder, recursive = true) should be('empty)
      listDirectoryContent(archiveDir, recursive = true) should contain theSameElementsAs Set(archiveDir.child(inputFile.relativePath))
    }

    it("should add a suffix increment to the target file name if the archive destination already exists") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempDir()
      val archiveConf = ArchiveConfiguration(copyTo = Some(archiveDir), removeInputFile = true)

      val originalTargetFile = archiveDir.child(inputFile.relativePath)
      createEmptyFile(originalTargetFile)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)
      archiveStrategy(inputFile.absolutePath, inputFile.relativePath)

      listDirectoryContent(archiveDir, recursive = true) should contain theSameElementsAs Set(
        originalTargetFile,
        originalTargetFile.parent.child(originalTargetFile.name + ".1"))
    }

    it("should just copy the input file to the archive folder if instructed to do so") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

      val archiveDir = createTempDir()
      val archiveConf = ArchiveConfiguration(copyTo = Some(archiveDir), removeInputFile = false)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)
      archiveStrategy(inputFile.absolutePath, inputFile.relativePath)

      listDirectoryContent(inputFolder, recursive = true) should contain theSameElementsAs Set(inputFile.absolutePath)
      listDirectoryContent(archiveDir, recursive = true) should contain theSameElementsAs Set(archiveDir.child(inputFile.relativePath))
    }

    it("should just delete the input file if instructed to do so") {
      val inputFolder = createTempDir()
      val inputFile = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)
      val archiveConf = ArchiveConfiguration(copyTo = None, removeInputFile = true)

      val archiveStrategy = HadoopArchiveStrategy(archiveConf)
      archiveStrategy(inputFile.absolutePath, inputFile.relativePath)

      listDirectoryContent(inputFolder, recursive = true) should be('empty)
    }
  }
}
