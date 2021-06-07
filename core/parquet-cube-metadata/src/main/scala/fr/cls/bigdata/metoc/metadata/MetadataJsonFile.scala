package fr.cls.bigdata.metoc.metadata

import fr.cls.bigdata.hadoop.model.PathWithFileSystem

import scala.io.Codec

object MetadataJsonFile {
  private final val fileName = "metadata.json"
  final val codec: Codec = Codec.UTF8

  def path(indexFolder: PathWithFileSystem): PathWithFileSystem = indexFolder.child(fileName)
}
