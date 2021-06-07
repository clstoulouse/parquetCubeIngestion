package fr.cls.bigdata.metoc.index

import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.io.Codec

object MetocIndex {
  final val FilesCodec = Codec.UTF8

  private final val TimeFileName = "time.lst"
  private final val DepthFileName = "depth.lst"
  private final val LatitudeFileName = "latitude.lst"
  private final val LongitudeFileName = "longitude.lst"

  def timeFile(indexPath: PathWithFileSystem): PathWithFileSystem = indexPath.child(MetocIndex.TimeFileName)
  def depthFile(indexPath: PathWithFileSystem): PathWithFileSystem = indexPath.child(MetocIndex.DepthFileName)
  def latitudeFile(indexPath: PathWithFileSystem): PathWithFileSystem = indexPath.child(MetocIndex.LatitudeFileName)
  def longitudeFile(indexPath: PathWithFileSystem): PathWithFileSystem = indexPath.child(MetocIndex.LongitudeFileName)
}
