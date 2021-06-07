package fr.cls.bigdata.hadoop.config

import com.typesafe.config.{Config, ConfigException}
import org.apache.hadoop.conf.Configuration

object HadoopConfiguration {
  private final val hadoopConfigurationKeyPath = "fr.cls.bigdata.hadoop.configuration"

  /**
    * Builds a [[org.apache.hadoop.conf.Configuration]] from an app configuration.
    *
    * @param config Configuration of the app.
    * @throws ConfigException if an error occurred while parsing the configuration.
    * @return a [[org.apache.hadoop.conf.Configuration]] built using your app configuration.
    */
  @throws[ConfigException]
  def apply(config: Config): Configuration = {
    val configSubset = config.getConfig(hadoopConfigurationKeyPath)

    val hadoopConfiguration = new Configuration()

    import scala.collection.JavaConverters._
    for {
      entry <- configSubset.entrySet().asScala
      key = entry.getKey
      configValue = entry.getValue
      unwrappedValue = configValue.unwrapped()
    } unwrappedValue match {
      case values: java.lang.Iterable[_] => hadoopConfiguration.setStrings(key, values.asScala.toSeq.map(_.toString): _*)
      case _ => hadoopConfiguration.set(key, unwrappedValue.toString)
    }

    hadoopConfiguration
  }
}
