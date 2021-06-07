package fr.cls.bigdata.netcdf.ucar.internal

import java.io.IOException
import java.net.URISyntaxException
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import fr.cls.bigdata.hadoop.HadoopIO
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.resource.Resource
import org.apache.hadoop.fs.FSDataInputStream
import ucar.unidata.io.RandomAccessFile

class HDFSRandomAccessFile(hdfsFile: PathWithFileSystem, bufferSize: Int) extends RandomAccessFile(bufferSize) {
  location = hdfsFile.toString()
  file = null

  override val length: Long = hdfsFile.fileSystem.getFileStatus(hdfsFile.path).getLen

  private val inputStream: Resource[FSDataInputStream] = try {
    HadoopIO.openInputStream(hdfsFile)
  } catch {
    case cause: URISyntaxException => throw new IOException(s"Provided URI is malformed: $hdfsFile")
  }

  override def close(): Unit = inputStream.close()

  override def read_(pos: Long, buff: Array[Byte], offset: Int, len: Int): Int = {
    inputStream.get.read(pos, buff, offset, len)
  }

  override def readToByteChannel(dest: WritableByteChannel, offset: Long, nbytes: Long): Long = {
    val buff = new Array[Byte](nbytes.toInt)
    val done = read_(offset, buff, 0, nbytes.toInt)
    dest.write(ByteBuffer.wrap(buff))
    done.toLong
  }
}

object HDFSRandomAccessFile {
  val defaultBufferSize: Int = 4 * 1024 // 4 ko
  def apply(hdfsFile: PathWithFileSystem): HDFSRandomAccessFile = new HDFSRandomAccessFile(hdfsFile, defaultBufferSize)
}
