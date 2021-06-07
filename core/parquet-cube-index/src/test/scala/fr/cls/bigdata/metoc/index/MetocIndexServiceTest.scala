package fr.cls.bigdata.metoc.index

import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.concurrent.DistributedLockingService
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

import scala.collection.SortedSet

class MetocIndexServiceTest extends FunSpec with MockFactory with Matchers with HadoopTestUtils {
  private val longValues = SortedSet(1L, 2L)
  private val otherLongValues = SortedSet(0L, 3L)
  private val doubleValues = SortedSet(3D, 4D)
  private val otherDoubleValues = SortedSet(5D, 6D)
  private final val indexFileName = "index-file"

  describe("writeImmutableDimensionIndex") {
    it("should create the index folder if it does not exist") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFolder = createTempDir().child("not-yet-created")
      val indexFilePath = indexFolder.child(indexFileName)

      service.writeImmutableDimensionIndex(indexFilePath, doubleValues)

      FileSystem.exists(indexFolder.path) shouldBe true
      FileSystem.isDirectory(indexFolder.path) shouldBe true
    }

    it("should write the index values when the index file does not exist") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFilePath = createTempDir().child(indexFileName)

      service.writeImmutableDimensionIndex(indexFilePath, doubleValues)

      val writtenValues = service.readDoubleValues(indexFilePath)
      writtenValues should contain theSameElementsAs doubleValues
    }

    it("should ignore the new values when the index file already exists") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFilePath = createTempDir().child(indexFileName)

      service.writeImmutableDimensionIndex(indexFilePath, doubleValues)
      service.writeImmutableDimensionIndex(indexFilePath, otherDoubleValues)

      val writtenValues = service.readDoubleValues(indexFilePath)
      writtenValues should contain theSameElementsAs doubleValues
    }
  }

  describe("writeMutableDimensionIndex") {
    it("should create the index folder if it does not exist") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFolder = createTempDir().child("not-yet-created")
      val indexFilePath = indexFolder.child(indexFileName)

      service.writeMutableDimensionIndex(indexFilePath, longValues)

      FileSystem.exists(indexFolder.path) shouldBe true
      FileSystem.isDirectory(indexFolder.path) shouldBe true
    }

    it("should write the index values when the index file does not exist") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFilePath = HadoopTestUtils.createTempDir().child(indexFileName)

      service.writeMutableDimensionIndex(indexFilePath, longValues)

      val writtenValues = service.readDoubleValues(indexFilePath)
      writtenValues should contain theSameElementsAs longValues
    }

    it("should merge the new values with the existing ones when the index file already exists") {
      val service = new MetocIndexService(passiveLockingService())
      val indexFilePath = HadoopTestUtils.createTempDir().child(indexFileName)

      service.writeMutableDimensionIndex(indexFilePath, longValues)
      service.writeMutableDimensionIndex(indexFilePath, otherLongValues)

      val writtenValues = service.readDoubleValues(indexFilePath)
      writtenValues should contain theSameElementsAs longValues ++ otherLongValues
    }
  }

  private def passiveLockingService(): DistributedLockingService = {
    val stubLockObject = stub[DistributedLockingService.Lock]
    (stubLockObject.close _).when().returns()

    val mockLockingService = stub[DistributedLockingService]
    (mockLockingService.lock _).when(*).returns(stubLockObject)

    mockLockingService
  }
}
