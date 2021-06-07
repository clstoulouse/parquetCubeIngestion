package fr.cls.bigdata.hadoop.config

import com.typesafe.config.{Config, ConfigException}
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration

case class ArchiveConfiguration(copyTo: Option[PathWithFileSystem], removeInputFile: Boolean) {
  /**
    * applies a transformation on the path to copy to if defined.
    *
    * @param f Transformation to apply.
    * @return Copy of the current object where the path (if any) is replaced with the path resulting from the transformation.
    */
  def mapCopyTo(f: PathWithFileSystem => PathWithFileSystem): ArchiveConfiguration = this.copy(copyTo.map(f), removeInputFile)
}

object ArchiveConfiguration {
  private final val copyToAttribute = "copy-to"
  private final val removeInputFileAttribute = "remove-input-file"

  @throws[ConfigException]
  def apply(subConfig: Config, hadoopConfig: Configuration): ArchiveConfiguration = {
    val copyTo = getOptionalString(subConfig, copyToAttribute).map(PathWithFileSystem(_, hadoopConfig))
    val removeInputFile = subConfig.getBoolean(removeInputFileAttribute)

    ArchiveConfiguration(copyTo, removeInputFile)
  }

  private def getOptionalString(subConfig: Config, attribute: String): Option[String] = {
    if (subConfig.hasPath(attribute)) {
      Some(subConfig.getString(attribute)).filter(StringUtils.isNotBlank)
    } else {
      None
    }
  }
}
