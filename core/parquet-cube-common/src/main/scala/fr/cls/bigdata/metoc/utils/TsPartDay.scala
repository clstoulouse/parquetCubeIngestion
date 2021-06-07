package fr.cls.bigdata.metoc.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

object TsPartDay {
  final val DimensionName = "tspartday"

  private val tsPartDayFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyDDD").withZone(ZoneOffset.UTC)

  def fromTime(time: Long): Long = {
    tsPartDayFormatter.format(Instant.ofEpochMilli(time)).toLong
  }
}
