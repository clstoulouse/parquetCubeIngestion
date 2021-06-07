package fr.cls.bigdata.hadoop.concurrent

import java.io.{File, IOException}
import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.config.DistributedLockingConfig
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs.Path

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

/**
  * Base trait that provides the functionality of distributed locking for files referenced by [[org.apache.hadoop.fs.Path]].
  */
trait DistributedLockingService {
  /**
    * Creates a distributed (inter-process) lock on the given path.
    * This method will return immediately when a first process/thread request a lock on a file.
    * If other process/threads request a lock, while the first one have not yet released it the call will block.
    * After the lock is released by the owner process/thread, another requester will be unblocked (there is no guarantee of fairness between requesters
    * in different process, the fairness holds only between threads of the same process).
    * If an owner process is killed, the file is considered released.
    * If a thread calls twice this method without unlocking, the same lock instance is returned immediately after the second call.
    *
    * @param pathToLock Path on which the lock is requested.
    * @return A lock object that can be used to release the lock.
    * @throws DistributedLockingException in case of error while trying to acquire the lock.
    */
  @throws(classOf[DistributedLockingException])
  def lock(pathToLock: Path): DistributedLockingService.Lock
}

object DistributedLockingService {

  private val repository = TrieMap.empty[DistributedLockingConfig, DistributedLockingService]

  def apply(config: DistributedLockingConfig): DistributedLockingService = {
    repository.getOrElseUpdate(config, new ConcurrentLockingService(config.lockFolder, config.stripeCount, config.timeoutMs))
  }


  /**
    * Object returned to a lock requester, that can be used to release its lock.
    */
  trait Lock extends Resource[Unit] {
    override def get: Unit = ()
  }

  /**
    * Implementation of [[DistributedLockingService]] that
    * distributes the locking task to multiple [[SynchronousLockingService]] based on the hash code of the path.
    * Two threads can lock two paths simultaneously iff the hash codes of the paths are different
    *
    * @param lockFolder  The posix folder where the file lock is created
    * @param stripeCount The number of stripes to allow concurrent locking in the same process
    */
  private class ConcurrentLockingService(lockFolder: File, stripeCount: Int, timeoutMs: Long) extends DistributedLockingService with LazyLogging {
    private val stripedLockingService = IndexedSeq.fill(stripeCount)(new SynchronousLockingService(lockFolder, timeoutMs))

    override def lock(pathToLock: Path): Lock = {
      val stripe = stripedLockingService(Math.abs(pathToLock.hashCode()) % stripeCount)
      stripe.lock(pathToLock)
    }
  }

  /**
    * Implementation of [[DistributedLockingService]] that relies on
    * posix file system to implement distributed locking.
    * Each time a lock is requested, a temporary file is created in a local filesystem and a posix lock is requested on it.
    * Inter-process synchronisation (between threads) is realised using [[java.util.concurrent.locks.ReentrantLock]].
    * Two threads of the same process cannot lock at the same time even if the paths are different.
    *
    * @param lockFolder The posix folder where the file lock is created
    */
  private class SynchronousLockingService(lockFolder: File, timeoutMs: Long) extends DistributedLockingService with LazyLogging {
    private val fileLocks = TrieMap.empty[Path, LockImpl]
    private val processLock = new ReentrantLock(true)

    override def lock(pathToLock: Path): Lock = {

      if (!processLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS))
        throw new DistributedLockingException(s"timeout while acquiring a lock on '$pathToLock'")


      val lock = try {
        fileLocks.getOrElseUpdate(pathToLock, unsafeDistributedLock(pathToLock))
      } catch {
        case ex: IOException =>
          processLock.unlock()
          throw new DistributedLockingException(s"error occurred while acquiring a lock on '$pathToLock'", ex)
      }

      lock.holdCount = lock.holdCount + 1
      lock
    }

    private def unsafeDistributedLock(pathToLock: Path): LockImpl = {

      val lockFilePath = Paths.get(lockFolder.toString, getLockFilePath(pathToLock))

      logger.debug(s"attempting to create lock file for '$pathToLock' at '$lockFilePath'...")

      Files.createDirectories(lockFilePath.getParent)

      val fileChannel = FileChannel.open(lockFilePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

      val start = System.currentTimeMillis()
      val fileLock = try {
        Iterator.continually(Option(fileChannel.tryLock()))
          .dropWhile { lock =>
            val retry = lock.isEmpty && (System.currentTimeMillis() - start) < timeoutMs
            if (retry) Thread.sleep(5)
            retry
          }.next()
          .getOrElse(throw new DistributedLockingException(s"timeout while acquiring a lock on '$lockFilePath'"))
      } catch {
        case NonFatal(cause) =>
          closeChannelSafely(fileChannel, s"Unable to close file channel for lock file '$lockFilePath'")
          throw cause
      }

      logger.debug(s"lock acquired for '$pathToLock' at '$lockFilePath'")

      new LockImpl(pathToLock, fileLock)
    }

    private def getLockFilePath(pathToLock: Path): String = {
      val filePath = pathToLock.toUri.getPath

      val hashCode = filePath.foldLeft(0L)((h, c) => 31 * h + c)
      val fileNamePrefix = filePath.take(15)
        .map(c => if (Character.isLetterOrDigit(c) || c == '-') c else '_')

      s"$fileNamePrefix-$hashCode.lock"
    }

    private class LockImpl(lockedPath: Path, fileLock: FileLock) extends Lock {
      var holdCount = 0

      override def close(): Unit = {
        holdCount = holdCount - 1

        if (holdCount == 0) {
          fileLocks.remove(lockedPath)
          try {
            fileLock.release()
          } catch {
            case ex: IOException =>
              throw new DistributedLockingException(s"error occurred while releasing a lock on file '$lockedPath'", ex)
          } finally {
            processLock.unlock()
            closeChannelSafely(fileLock.channel(), s"Unable to close file channel for lock on file '$lockedPath'")
          }
        } else {
          processLock.unlock()
        }
      }
    }

    private def closeChannelSafely(fileChannel: FileChannel, message: => String): Unit = {
      try {
        fileChannel.close()
      } catch {
        case ex: IOException => logger.warn(message, ex)
      }
    }
  }

}
