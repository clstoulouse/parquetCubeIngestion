package fr.cls.bigdata.metoc.index

import java.io.{IOException, OutputStreamWriter, PrintWriter}

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.hadoop.concurrent.{DistributedLockingException, DistributedLockingService}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.{MetocReaderException, MetocWriterException}
import fr.cls.bigdata.resource.Resource
import org.apache.commons.lang.StringUtils

import scala.collection.SortedSet
import scala.io.Source
import scala.reflect.runtime.universe.TypeTag

private class MetocIndexService(lockingService: DistributedLockingService)
  extends LazyLogging {

  @throws[MetocWriterException]
  def writeMutableDimensionIndex(indexFile: PathWithFileSystem, values: SortedSet[Long]): Unit = {
    try {
      for (_ <- lockingService.lock(indexFile.path)) {
        val oldValues: SortedSet[Long] = if (HadoopIO.exists(indexFile)) {
          logger.debug(s"reading old values from index file for mutable dimension before the update -> '$indexFile'...")
          unsafeReadValues[Long](indexFile, _.toLong)
        } else SortedSet.empty
        val newValues = oldValues ++ values
        logger.debug(s"updating index file for mutable dimension (old-values = ${oldValues.size}, new-values = ${newValues.size}) -> '$indexFile'")
        unsafeWriteValues[Long](indexFile, newValues)
        logger.debug(s"created/updated index file for mutable dimension -> $indexFile")
      }
    } catch {
      case t: MetocReaderException => throw new MetocWriterException("unable to read index for update", t)
      case t: IOException => throw new MetocWriterException(s"Error while writing in index file $indexFile", t)
      case t: DistributedLockingException =>
        throw new MetocWriterException(s"unable to synchronize when accessing index $indexFile", t)
    }
  }

  @throws[MetocWriterException]
  def writeImmutableDimensionIndex(indexFile: PathWithFileSystem, values: SortedSet[Double]): Unit = {
    try {
      if (HadoopIO.exists(indexFile)) {
        logger.debug(s"index file for immutable dimension already exists, it won't be updated: $indexFile")
      } else {
        for (_ <- lockingService.lock(indexFile.path)) {
          if (!HadoopIO.exists(indexFile)) {
            logger.debug(s"creating index file for immutable dimension -> '$indexFile'...")
            unsafeWriteValues[Double](indexFile, values)
            logger.debug(s"created index file for immutable dimension -> $indexFile")
          }
        }
      }
    } catch {
      case t: IOException => throw new MetocWriterException(s"Error while writing in index file $indexFile", t)
      case t: DistributedLockingException =>
        throw new MetocWriterException(s"Unable to synchronize when writing index $indexFile", t)
    }
  }

  @throws[MetocReaderException]
  def readLongValues(indexFilePath: PathWithFileSystem): SortedSet[Long] = readValues[Long](indexFilePath, _.toLong)

  @throws[MetocReaderException]
  def readDoubleValues(indexFilePath: PathWithFileSystem): SortedSet[Double] = readValues[Double](indexFilePath, _.toDouble)

  private def readValues[T: Ordering: TypeTag](indexFile: PathWithFileSystem, parser: String => T): SortedSet[T] = {
    try {
      unsafeReadValues(indexFile, parser)
    } catch {
      case cause: IOException =>
        throw new MetocReaderException(s"Error while reading index file $indexFile", cause)
      case cause: NumberFormatException =>
        throw new MetocReaderException(s"Unable to parse value of type ${implicitly[TypeTag[T]]} in index file $indexFile'", cause)
    }
  }

  private def unsafeWriteValues[T](indexFile: PathWithFileSystem, values: Traversable[T]): Unit = {
    for {
      outputStream <- HadoopIO.createOutputStream(indexFile, overwrite = true)
      outputStreamWriter = new OutputStreamWriter(outputStream, MetocIndex.FilesCodec.charSet)
      writer <- Resource(new PrintWriter(outputStreamWriter, true))
      value <- values
    } writer.println(value.toString)
  }

  private def unsafeReadValues[T: Ordering](indexFile: PathWithFileSystem, parser: String => T): SortedSet[T] = {
    val iterator = for {
      stream <- HadoopIO.openInputStream(indexFile)
    } yield for {
      line <- Source.fromInputStream(stream)(MetocIndex.FilesCodec).getLines()
      trimmedLine = StringUtils.trim(line) if StringUtils.isNotBlank(trimmedLine)
    } yield parser(trimmedLine)

    iterator.acquire(values => SortedSet(values.toSeq: _*))
  }
}
