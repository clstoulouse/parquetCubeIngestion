package fr.cls.bigdata.hadoop.config

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigException}

case class DistributedLockingConfig(lockFolder: File, stripeCount: Int, timeoutMs: Long)

object DistributedLockingConfig {
  private final val configPath = "fr.cls.bigdata.hadoop.distributed-lock"
  private final val lockFolderAttribute = "lock-folder"
  private final val stripeCountAttribute = "stripe-count"
  private final val timeoutAttribute = "timeout"

  /**
    * Creates a [[DistributedLockingConfig]] from app configuration.
    *
    * @param config app configuration.
    * @throws ConfigException if the configuration is invalid.
    * @return A [[DistributedLockingConfig]].
    */
  @throws[ConfigException]
  def apply(config: Config): DistributedLockingConfig = {
    val subConfig = config.getConfig(configPath)
    DistributedLockingConfig(
      lockFolder = new File(subConfig.getString(lockFolderAttribute)),
      stripeCount = subConfig.getInt(stripeCountAttribute),
      timeoutMs = subConfig.getDuration(timeoutAttribute, TimeUnit.MILLISECONDS)
    )
  }
}
