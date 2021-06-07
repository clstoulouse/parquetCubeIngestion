package fr.cls.bigdata.hadoop

import java.io.IOException

import fr.cls.bigdata.hadoop.model.AbsoluteAndRelativePath
import org.scalatest.{FunSpec, Matchers}

class HadoopIOSpec extends FunSpec with Matchers with HadoopTestUtils {
  describe("initFolder") {
    it("should do nothing if the folder exists") {
      val folder = createTempDir()
      HadoopIO.initFolder(folder)
    }

    it("should throw an IOException if a file exists in the location of the folder") {
      val folder = createTempFile()
      an[IOException] should be thrownBy HadoopIO.initFolder(folder)
    }

    it("should throw an IOException if the folder does not exist") {
      val folder = createTempDir().child("non-existent")
      an[IOException] should be thrownBy HadoopIO.initFolder(folder)
    }
  }

  describe("createFolderIfNotExist") {
    it("should check the folder exists") {
      val folder = createTempDir()
      HadoopIO.createFolderIfNotExist(folder)
    }

    it("should throw an IOException if a file exists in the location of the folder") {
      val folder = HadoopTestUtils.createTempFile()
      an[IOException] should be thrownBy HadoopIO.createFolderIfNotExist(folder)
    }

    it("should create the folder if it does not exist") {
      val folder = createTempDir().child("non-existent")
      HadoopIO.createFolderIfNotExist(folder)

      FileSystem.exists(folder.path) shouldBe true
      FileSystem.isDirectory(folder.path) shouldBe true
    }

    it("should throw an IOException if the folder cannot be created") {
      val folder = createTempDir().child("non-existent")
      simulateNoWritePermissionsOn(folder.parent)

      an[IOException] should be thrownBy HadoopIO.createFolderIfNotExist(folder)
    }
  }

  describe("createFolder") {
    it("should throw an IOException if the folder exists") {
      val folder = createTempDir()
      an[IOException] should be thrownBy HadoopIO.createFolder(folder)
    }

    it("should create the folder") {
      val folder = createTempDir().child("non-existent")

      HadoopIO.createFolder(folder)

      HadoopTestUtils.FileSystem.exists(folder.path) shouldBe true
      HadoopTestUtils.FileSystem.isDirectory(folder.path) shouldBe true
    }

    it("should throw an IOException if the folder cannot be created") {
      val folder =createTempDir().child("non-existent")
      simulateNoWritePermissionsOn(folder.parent)
      an[IOException] should be thrownBy HadoopIO.createFolder(folder)
    }
  }

  describe("listFilesInFolder()") {
    it("should list the content of the input folder (recursive = false)") {
      val inputFolder = createTempDir()

      val file1 = createTempFile(inputFolder)
      val file2 = createTempFile(inputFolder)
      createTempFile(inputFolder.child("sub-folder"))

      val foundFiles = HadoopIO.listFilesInFolder(inputFolder, recursive = false).toSet

      foundFiles should contain theSameElementsAs Set(
        AbsoluteAndRelativePath(inputFolder, file1.path),
        AbsoluteAndRelativePath(inputFolder, file2.path)
      )
    }

    it("should list the content of the input folder (recursive = true)") {
      val inputFolder = createTempDir()

      val file1 = createTempFile(inputFolder)
      val file2 = createTempFile(inputFolder)
      val file3 = createTempFile(inputFolder.child("sub-folder"))

      val foundFiles = HadoopIO.listFilesInFolder(inputFolder, recursive = true).toSet

      foundFiles should contain theSameElementsAs Set(
        AbsoluteAndRelativePath(inputFolder, file1.path),
        AbsoluteAndRelativePath(inputFolder, file2.path),
        AbsoluteAndRelativePath(inputFolder, file3.path)
      )
    }
  }

  describe("listSubFolders()") {
    it("should list the subFolder of the input folder (recursive = false)") {
      val inputFolder = createTempDir()
      val subFolder = inputFolder.child("folder")

      createTempFile(inputFolder)
      HadoopIO.createFolder(subFolder)

      HadoopIO.listSubFolders(inputFolder).toSeq should contain theSameElementsAs Set(
        AbsoluteAndRelativePath(inputFolder, subFolder.path)
      )
    }
  }

  describe(".deleteFile()") {
    it("should return silently if the file does not exist") {
      val inputFolder = HadoopTestUtils.createTempDir()

      HadoopIO.deleteFile(inputFolder.child("file.txt"))

      val paths = HadoopTestUtils.listDirectoryContent(inputFolder, recursive = true)
      paths should be('empty)
    }

    it("should delete a file if it exists") {
      val inputFolder = HadoopTestUtils.createTempDir()
      val file = inputFolder.child("file.txt")
      HadoopTestUtils.createEmptyFile(file)

      HadoopIO.deleteFile(file)

      val paths = HadoopTestUtils.listDirectoryContent(inputFolder, recursive = true)
      paths should be('empty)
    }
  }

  describe("lastModificationMillis") {
    it("should return creation timestamp in milliseconds") {
      val file = HadoopTestUtils.createTempDir().child("file")
      val now = System.currentTimeMillis()

      HadoopTestUtils.createEmptyFile(file)
      val lastModification = HadoopIO.lastModificationMillis(file)

      lastModification should be (now +- 1000)
    }
  }
}
