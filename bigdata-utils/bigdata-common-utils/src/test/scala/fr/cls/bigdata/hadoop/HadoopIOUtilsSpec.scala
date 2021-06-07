package fr.cls.bigdata.hadoop

import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class HadoopIOUtilsSpec extends FunSpec with MockFactory with Matchers {
  val parentFolder = PathWithFileSystem(new Path("parent"), HadoopTestUtils.FileSystem)
  val fileName = "some.file"
  val targetFile: PathWithFileSystem = parentFolder.child(fileName)

  val folderName = "folder"
  val targetFolder: PathWithFileSystem = parentFolder.child(folderName)

  describe("generateUniquePath") {
    it("should return the target path when the target file does not exist") {
      val mockIO = stub[HadoopIO]
      (mockIO.exists _).when(targetFile).returns(false)

      val ioUtils = new HadoopIOUtils(mockIO)

      val uniquePath = ioUtils.generateUniquePath(parentFolder, fileName)
      uniquePath shouldBe targetFile
    }

    it("should add an increment when the target file does not exist") {
      // prepare
      val mockIO = stub[HadoopIO]
      (mockIO.exists _).when(targetFile).returns(true)
      (mockIO.exists _).when(*).returns(false)

      val ioUtils = new HadoopIOUtils(mockIO)

      val uniquePath = ioUtils.generateUniquePath(parentFolder, fileName)
      uniquePath shouldBe parentFolder.child(s"$fileName.1")
    }
  }

  describe("createTemporaryFolder") {
    it("should not create the folder when createFolder is set to false") {
      val mockIO = stub[HadoopIO]
      (mockIO.exists _).when(targetFolder).returns(false)
      (mockIO.createFolder _).when(targetFolder).never()

      val ioUtils = new HadoopIOUtils(mockIO)

      ioUtils.manageTemporaryFolder(parentFolder, folderName, createFolder = false)
    }

    it("should create the folder when createFolder is set to true") {
      val mockIO = stub[HadoopIO]
      (mockIO.exists _).when(targetFolder).returns(false)
      (mockIO.createFolder _).when(targetFolder).once()

      val ioUtils = new HadoopIOUtils(mockIO)

      ioUtils.manageTemporaryFolder(parentFolder, folderName, createFolder = true)
    }

    describe("close") {
      it("should not delete the folder if it has not been created") {
        val mockIO = stub[HadoopIO]
        (mockIO.exists _).when(targetFolder).returns(false)
        (mockIO.deleteFolder _).when(targetFolder).never()

        val ioUtils = new HadoopIOUtils(mockIO)

        val tempResource = ioUtils.manageTemporaryFolder(parentFolder, folderName, createFolder = false)
        tempResource.foreach(_ => ())
      }

      it("should delete the folder if it has been created") {
        val mockIO = stub[HadoopIO]
        inSequence {
          (mockIO.exists _).when(targetFolder).returns(false).once()
          (mockIO.createFolder _).when(targetFolder).once()
          (mockIO.deleteFolder _).when(targetFolder).once()
        }

        val ioUtils = new HadoopIOUtils(mockIO)

        val tempResource = ioUtils.manageTemporaryFolder(parentFolder, folderName, createFolder = true)
        tempResource.foreach(_ => ())
      }
    }
  }
}
