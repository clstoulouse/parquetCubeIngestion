package fr.cls.bigdata.metoc.ingestion.logging

import fr.cls.bigdata.logging.LoggingContext
import org.slf4j.LoggerFactory

object CrawlingTracer {
  private val logger = LoggerFactory.getLogger("ingestion-crawler-trace")

  def onFileStart(): Unit = {
    for (_ <- LoggingContext(MdcKeys.LifecycleEventType, "start")) {
      logger.info("ingestion file...")
    }
  }

  def onFileSuccess(ingestionTime: Long): Unit = {
    for {
      _ <- LoggingContext(MdcKeys.LifecycleEventType, "completed")
      _ <- LoggingContext(MdcKeys.CompletionType, "success")
      _ <- LoggingContext(MdcKeys.ExecutionTime, String.valueOf(ingestionTime))
    } {
      logger.info("file successfully ingested")
    }
  }

  def onFileFailure(t: Throwable): Unit = {
    for {
      _ <- LoggingContext(MdcKeys.LifecycleEventType, "completed")
      _ <- LoggingContext(MdcKeys.CompletionType, "failure")
    } {
      logger.info("failed to ingest file", t)
    }
  }

  def onFileArchivingFailure(t: Throwable): Unit = {
    for {
      _ <- LoggingContext(MdcKeys.LifecycleEventType, "completed")
      _ <- LoggingContext(MdcKeys.CompletionType, "failure")
    } {
      logger.info("failed to archive file", t)
    }
  }

}
