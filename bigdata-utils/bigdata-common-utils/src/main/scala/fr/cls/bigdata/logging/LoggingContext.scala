package fr.cls.bigdata.logging

import fr.cls.bigdata.resource.Resource
import org.slf4j.MDC

class LoggingContext private (key: String) extends AutoCloseable {
  def close(): Unit = MDC.remove(key)
}

object LoggingContext {
  def apply(key: String, value: String): Resource[LoggingContext] = {
    MDC.put(key, value)
    Resource(new LoggingContext(key))
  }
}
