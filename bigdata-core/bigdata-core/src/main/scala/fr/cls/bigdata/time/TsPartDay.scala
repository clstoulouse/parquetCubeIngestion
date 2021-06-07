package fr.cls.bigdata.time

import java.time.{DateTimeException, Instant, LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter

@throws[DateTimeException]
case class TsPartDay(year: Int, day: Int) {
  import TsPartDay._

  /**
    * the date value is computed at construction
    * in order to throw a DateTimeException when the year and day values are invalid (eg: '2019366')
    */
  val date: LocalDate = LocalDate.ofYearDay(year, day)

  def stamp: Int = year * 1000 + day

  def instant: Instant = date.atStartOfDay(ZoneOffset.UTC).toInstant

  def epochMilli: Long = instant.toEpochMilli

  def minusDays(days: Int): TsPartDay = fromDate(date.minusDays(days))

  def plusDays(days: Int): TsPartDay = fromDate(date.plusDays(days))

  def until(endExclusive: TsPartDay): Seq[TsPartDay] = {
    epochMilli.until(endExclusive.epochMilli).by(millisPerDay).map(fromEpochMilli)
  }

  def to(endInclusive: TsPartDay): Seq[TsPartDay] = {
    epochMilli.to(endInclusive.epochMilli).by(millisPerDay).map(fromEpochMilli)
  }

  override def toString: String = f"$year%04d$day%03d"
}

object TsPartDay {
  final val millisPerDay = 1000 * 60 * 60 * 24

  private val format = DateTimeFormatter.ofPattern("yyyyDDD").withZone(ZoneOffset.UTC)

  def today: TsPartDay = fromDate(LocalDate.now(ZoneOffset.UTC))

  def fromStamp(stamp: String): TsPartDay = fromDate(LocalDate.from(format.parse(stamp)))
  def fromEpochMilli(time: Long): TsPartDay = fromDate(Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).toLocalDate)
  def fromDate(date: LocalDate): TsPartDay = TsPartDay(date.getYear, date.getDayOfYear)
}
