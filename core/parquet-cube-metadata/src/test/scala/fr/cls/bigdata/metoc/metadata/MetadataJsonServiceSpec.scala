package fr.cls.bigdata.metoc.metadata

import java.io.IOException

import fr.cls.bigdata.georef.metadata.MetadataAttribute
import fr.cls.bigdata.georef.model.DataType
import fr.cls.bigdata.hadoop.concurrent.DistributedLockingService
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.hadoop.{HadoopIO, HadoopTestUtils}
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.metadata.diff.MetadataComparisonService
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers, OptionValues}

class MetadataJsonServiceSpec extends FunSpec with Matchers with MockFactory with OptionValues with HadoopTestUtils with UnitTestData {

  describe("raw read/write") {
    it("should read/write metadata files without alteration") {
      val testFile = MetadataJsonFile.path(createTempDir())

      MetadataJsonService.writeMetadataFile(testFile, Dataset.metadata)

      val readMetadata = MetadataJsonService.readMetadataFile(testFile)

      readMetadata shouldBe Dataset.metadata
    }
  }

  it("should write and read metadata.json from/to disk") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadataFilePath = MetadataJsonFile.path(indexFolder)

    val metadataService = createJsonService()
    metadataService.write(settings, Dataset.metadata)
    val readMetadata = metadataService.readAll(settings)

    HadoopIO.exists(metadataFilePath) shouldBe true
    readMetadata shouldBe Dataset.metadata
  }

  it("should do nothing if the metadata did NOT change") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadataFilePath = MetadataJsonFile.path(indexFolder)

    val metadataService = createJsonService()
    metadataService.write(settings, Dataset.metadata)
    metadataService.write(settings, Dataset.metadata)

    HadoopIO.exists(metadataFilePath) shouldBe true
    metadataService.tryReadMetadataFile(metadataFilePath).value shouldBe Dataset.metadata
    listDirectoryContent(metadataFilePath.parent, recursive = true) should contain theSameElementsAs Set(metadataFilePath)
  }

  it(s"should write a new version of the file and back up the old one when the metadata has non-breaking changes") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadataFile = MetadataJsonFile.path(indexFolder)

    val backup1File = metadataFile.map(f => new Path(f + ".1"))
    val backup2File = metadataFile.map(f => new Path(f + ".2"))

    val metadata1 = Dataset.metadata.copy(attributes = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3)), MetadataAttribute("attr2", DataType.Int, Seq(1, 2, 3))))
    val metadata2 = Dataset.metadata.copy(attributes = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3)), MetadataAttribute("attr2", DataType.Int, Seq(1, 2, 3, 4))))
    val metadata3 = Dataset.metadata.copy(attributes = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3, 4)), MetadataAttribute("attr2", DataType.Int, Seq(1, 2, 3))))

    val metadataService = createJsonService()
    metadataService.write(settings, metadata1)
    metadataService.write(settings, metadata2)
    metadataService.write(settings, metadata3)

    HadoopIO.exists(backup1File) shouldBe true
    metadataService.tryReadMetadataFile(backup1File).value shouldBe metadata1

    HadoopIO.exists(backup2File) shouldBe true
    metadataService.tryReadMetadataFile(backup2File).value shouldBe Dataset.metadata.copy(attributes = Set(MetadataAttribute("attr1", DataType.Int, Seq(1, 2, 3))))

    HadoopIO.exists(metadataFile) shouldBe true
    metadataService.tryReadMetadataFile(metadataFile).value shouldBe Dataset.metadata.copy(attributes = Set.empty)
  }

  it("should do nothing when the metadata contain non significant changes") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadataFilePath = MetadataJsonFile.path(indexFolder)

    val metadataService = createJsonService()
    val metadata1 = Dataset.metadata
    val metadata2 = metadata1.copy(attributes = metadata1.attributes + MetadataAttribute("attrX", DataType.Int, Seq(1, 2, 3)))

    metadataService.write(settings, metadata1)
    metadataService.write(settings, metadata2)

    HadoopIO.exists(metadataFilePath) shouldBe true
    metadataService.tryReadMetadataFile(metadataFilePath).value shouldBe metadata1
    listDirectoryContent(metadataFilePath.parent, recursive = true) should contain theSameElementsAs Set(metadataFilePath)
  }

  it(s"should throw ${classOf[MetocWriterException].getSimpleName} when breaking changes are detected") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadata1 = Dataset.metadata.copy(dimensions = Map(Longitude.ref -> Longitude.metadata))
    val metadata2 = Dataset.metadata.copy(dimensions = Map(Longitude.ref.copy(name = "other-dimension") -> Longitude.metadata))

    val metadataService = createJsonService()
    metadataService.write(settings, metadata1)

    a[MetocWriterException] should be thrownBy metadataService.write(settings, metadata2)
  }

  it("should read metadata of the specified variables") {
    val indexFolder = createTempDir()
    val settings = createDatasetSettings(indexFolder)
    val metadataFilePath = MetadataJsonFile.path(indexFolder)

    val metadataService = createJsonService()
    metadataService.write(settings, Dataset.metadata)
    val readMetadata = metadataService.read(settings, Seq(Variable3D.ref))

    HadoopIO.exists(metadataFilePath) shouldBe true
    readMetadata.variables shouldBe Map(Variable3D.ref -> Variable3D.metadata)
    readMetadata.dimensions.keySet shouldBe Variable3D.metadata.dimensions.toSet
  }

  private def createDatasetSettings(indexFolder: PathWithFileSystem): DatasetSettings = {
    DatasetSettings("dataset name", dataFolder = PathWithFileSystem("data path", HadoopConfiguration), indexFolder)
  }

  private def createJsonService(): MetadataJsonService = {
    new MetadataJsonService(passiveLockingService(), HadoopConfiguration, MetadataComparisonService)
  }

  private def passiveLockingService(): DistributedLockingService = {
    val stubLockObject = stub[DistributedLockingService.Lock]
    (stubLockObject.close _).when().returns().anyNumberOfTimes()

    val mockLockingService = stub[DistributedLockingService]
    (mockLockingService.lock _).when(*).returns(stubLockObject).anyNumberOfTimes()

    mockLockingService
  }

}
