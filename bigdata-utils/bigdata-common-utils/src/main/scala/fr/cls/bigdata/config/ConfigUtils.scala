package fr.cls.bigdata.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.{LazyLogging, Logger}

object ConfigUtils extends LazyLogging{
  private val options = ConfigRenderOptions.defaults().setOriginComments(false).setJson(false).setFormatted(true)

  /**
    * Prints the config in an easy to read format for logging.
    *
    * @param config Configuration to render.
    * @return Rendered configuration.
    */
  def renderConfig(config: Config): String = {
    config.root().render(options)
  }

  def loadPrintAndCheckConfig(logger: Logger, config: => Config = ConfigFactory.load()): Config = {
    val renderedConfig = renderConfig(config)
    logger.info(s"Application config:\n$renderedConfig")

    val reference = ConfigFactory.defaultReference()
    config.checkValid(reference)
    config
  }

  def loadAndCheckConfig(config: => Config = ConfigFactory.load()): Config = {
    logger.debug(s"Application config:\n${renderConfig(config)}")
    val reference = ConfigFactory.defaultReference()
    config.checkValid(reference)
    config
  }
}
