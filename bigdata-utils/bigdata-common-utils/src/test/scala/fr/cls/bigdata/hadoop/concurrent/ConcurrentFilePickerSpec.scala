package fr.cls.bigdata.hadoop.concurrent

import java.io.IOException

import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.AbsoluteAndRelativePath
import org.scalatest.{FunSpec, Matchers, OptionValues}

class ConcurrentFilePickerSpec extends FunSpec with Matchers with OptionValues with HadoopTestUtils {
  private val inProgressFolderName = ".test-in-progress"

  describe("ConcurrentHadoopFilesPicker") {

    describe(".pickExclusivelyOneFile()") {
      it("should return None if there is no file to pick up") {
        val inputFolder = createTempDir()

        val filePicker = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        val result = filePicker.pickOneFile(recursive = true, _ => true)

        result shouldBe None
      }

      it("should create in-progress folder if it does not exist") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)
        createTempFile(inputFolder)

        val filePicker = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        filePicker.pickOneFile(recursive = true, _ => true)

        FileSystem.exists(inProgressFolder.path) shouldBe true
        FileSystem.isDirectory(inProgressFolder.path) shouldBe true
      }

      it("should throw an IOException if unable to create in-progress folder") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)

        createTempFile(inputFolder)
        simulateNoWritePermissionsOn(inProgressFolder.parent)

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)

        an[IOException] should be thrownBy service.pickOneFile(recursive = true, _ => true)
      }

      it("should throw an IOException if unable to move picked up file to in progress folder") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)

        FileSystem.mkdirs(inProgressFolder.path)
        simulateNoWritePermissionsOn(inProgressFolder)
        createTempFile(inputFolder)

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)

        an[IOException] should be thrownBy service.pickOneFile(recursive = true, _ => true)
      }

      it("should pick a file and move it to in progress folder") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)

        val file = AbsoluteAndRelativePath(inputFolder, createTempFile(inputFolder).path)

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        val result = service.pickOneFile(recursive = true, _ => true)

        result.value.absolutePath shouldBe inProgressFolder.child(file.relativePath)
        result.value.relativePath shouldBe file.relativePath
        listDirectoryContent(inputFolder, recursive = true) should contain theSameElementsAs Set(result.value.absolutePath)
      }

      it("should ignore files filtered out by the predicate") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)
        val file1 = createTempFile(inputFolder)
        val file2 = createTempFile(inputFolder)

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        val result = service.pickOneFile(recursive = true, _.path == file2.path)

        result.value.absolutePath shouldBe inProgressFolder.child(file2.name)
        result.value.relativePath shouldBe file2.name
        listDirectoryContent(inputFolder, recursive = true) should contain theSameElementsAs Set(file1, result.value.absolutePath)
      }

      it("should ignore files in sub-folders when recursive = false") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)
        val file1 = createTempFile(inputFolder)
        val file2 = createTempFile(inputFolder.child("sub-folder"))

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        val result = service.pickOneFile(recursive = false, _ => true)

        result.value.absolutePath shouldBe inProgressFolder.child(file1.name)
        result.value.relativePath shouldBe file1.name
        listDirectoryContent(inputFolder, recursive = true) should contain theSameElementsAs Set(file2, result.value.absolutePath)
      }

      it("should ignore files in in-progress folder") {
        val inputFolder = createTempDir()
        val inProgressFolder = inputFolder.child(inProgressFolderName)
        createTempFile(inProgressFolder)

        val service = ConcurrentFilePicker(inputFolder, inProgressFolderName)
        val result = service.pickOneFile(recursive = true, _ => true)

        result shouldBe None
      }
    }
  }
}
