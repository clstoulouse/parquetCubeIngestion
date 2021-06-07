package fr.cls.bigdata.time

trait Timer {
  protected def elapsedTimeMs(f: => Unit): Long = {
    val startTime = System.currentTimeMillis()
    f
    System.currentTimeMillis() - startTime
  }
}

object Timer extends Timer {
  override def elapsedTimeMs(f: => Unit): Long = super.elapsedTimeMs(f)
}
