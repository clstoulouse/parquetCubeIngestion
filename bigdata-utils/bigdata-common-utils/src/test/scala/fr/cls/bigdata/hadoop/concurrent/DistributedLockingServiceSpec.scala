package fr.cls.bigdata.hadoop.concurrent

import java.io.File

import fr.cls.bigdata.hadoop.{FileTestUtils, HadoopTestUtils}
import fr.cls.bigdata.hadoop.config.DistributedLockingConfig
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.{Await, Future}


class DistributedLockingServiceSpec extends FunSpec with Matchers with ScalaFutures with FileTestUtils {
  import scala.concurrent.ExecutionContext.Implicits._
  import scala.concurrent.duration._

  describe("DistributedLockingService") {
    it(s"should create a lock folder when it does not exist") {
      val lockFolder = new File(createTempDir(), "non-existent")
      val service = DistributedLockingService(DistributedLockingConfig(lockFolder, 10, 1000))

      service.lock(new Path("file1"))

      lockFolder.exists() shouldBe true
      lockFolder.isDirectory shouldBe true
    }


    it(s"should throw a ${classOf[DistributedLockingException].getSimpleName} if unable to create the lock") {
      val lockFolder = createTempDir()
      val service = DistributedLockingService(DistributedLockingConfig(lockFolder, 10, 1000))
      HadoopTestUtils.simulateNoPermissionsOn(HadoopTestUtils.toHadoopPath(lockFolder))

      a[DistributedLockingException] should be thrownBy service.lock(new Path("file1"))
    }

    it("should create different locks for different input files") {
      val service = distributedLockingService()

      val lock1 = service.lock(new Path("file1"))
      val lock2 = service.lock(new Path("file2"))

      lock1 should not be lock2
    }

    it("should return the same lock when the caller thread already owns the lock") {
      val service = distributedLockingService()

      val lock1 = service.lock(new Path("file1"))
      val lock2 = service.lock(new Path("file1"))

      lock1 shouldBe theSameInstanceAs(lock2)
    }

    it("should create a new lock on the same file if the first one was released") {
      val service = distributedLockingService()

      val lock1 = service.lock(new Path("file1"))
      lock1.close()
      val lock2 = service.lock(new Path("file1"))

      lock1 should not be lock2
    }

    it("should block another thread from locking a file if another thread owns it") {
      val service = distributedLockingService()

      service.lock(new Path("file1"))
      val futureLock2 = Future(service.lock(new Path("file1")))

      futureLock2.isReadyWithin(500.milliseconds) shouldBe false // this is not perfect, i know
    }

    it("should unblock the requester thread when the owner thread releases the lock") {
      val service = distributedLockingService()

      val lock1 = service.lock(new Path("file1"))
      val futureLock2 = Future(service.lock(new Path("file1")))
      lock1.close()

      futureLock2.isReadyWithin(500.milliseconds) shouldBe true
    }

    it("should block the requester thread when the owner thread does not release all locks") {
      val service = distributedLockingService()
      val concurrentPath = new Path("path")

      service.lock(concurrentPath)
      service.lock(concurrentPath).close()

      val futureLock = Future(service.lock(concurrentPath))

      futureLock.isReadyWithin(500.milliseconds) shouldBe false
    }

    it("should acquire lock in the same thread") {
      val service = distributedLockingService()
      val concurrentPath = new Path("path")

      val futureLock = Future {
        service.lock(concurrentPath)
        service.lock(concurrentPath).close()
        service.lock(concurrentPath)
      }

      futureLock.isReadyWithin(1.seconds) shouldBe true
    }

    it("should timeout when the caller thread do not unlock the path in time") {
      val service = distributedLockingService()
      val concurrentPath = new Path("path")

      service.lock(concurrentPath)
      val futureLock = Future(service.lock(concurrentPath))

      a[DistributedLockingException] should be thrownBy Await.result(futureLock, 2.seconds)
    }
  }

  def distributedLockingService(): DistributedLockingService = {
    val lockFolder = createTempDir()
    DistributedLockingService(DistributedLockingConfig(lockFolder, 10, 1000))
  }


}
