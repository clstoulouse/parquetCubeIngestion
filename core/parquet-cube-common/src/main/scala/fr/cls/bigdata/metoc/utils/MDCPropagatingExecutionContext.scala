package fr.cls.bigdata.metoc.utils

import java.util

import org.slf4j.MDC

import scala.concurrent.ExecutionContext

/**
  * Wrapper around [[scala.concurrent.ExecutionContext]] that preserves the mdc of the calling thread inside the worker threads.
  */
abstract class MDCPropagatingExecutionContext(inner: ExecutionContext) extends ExecutionContext {

  protected def runnableMdc: util.Map[String, String]

  override def prepare(): ExecutionContext = {
    new MDCPropagatingExecutionContext.ExecutionContextWithStaticMDC(inner.prepare(), MDC.getCopyOfContextMap)
  }

  override def execute(runnable: Runnable): Unit = inner.execute(new MDCPropagatingExecutionContext.MDCRunnableWrapper(runnable, runnableMdc))

  override def reportFailure(cause: Throwable): Unit = inner.reportFailure(cause)
}

object MDCPropagatingExecutionContext {

  def apply(inner: ExecutionContext): MDCPropagatingExecutionContext = inner match {
    case mdcEc: MDCPropagatingExecutionContext => mdcEc
    case _ => new ExecutionContextWithDynamicMDC(inner)
  }

  private class ExecutionContextWithDynamicMDC(inner: ExecutionContext) extends MDCPropagatingExecutionContext(inner) {

    override protected def runnableMdc: util.Map[String, String] = MDC.getCopyOfContextMap
  }

  private class ExecutionContextWithStaticMDC(inner: ExecutionContext, protected override val runnableMdc: java.util.Map[String, String]) extends MDCPropagatingExecutionContext(inner)

  private class MDCRunnableWrapper(inner: Runnable, mdc: java.util.Map[String, String]) extends Runnable {
    override def run(): Unit = {
      val oldMdc = MDC.getCopyOfContextMap
      if(mdc != null) {
        MDC.setContextMap(mdc)
      }
      try {
        inner.run()
      } finally {
        if(oldMdc != null) {
          MDC.setContextMap(oldMdc)
        } else {
          MDC.clear()
        }
      }
    }
  }

}
