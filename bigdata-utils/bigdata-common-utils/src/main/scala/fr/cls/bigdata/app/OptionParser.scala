package fr.cls.bigdata.app

import java.time.Duration

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.build.BuildInfo
import org.apache.hadoop.fs.Path
import scopt.Read

import scala.concurrent.duration

abstract class OptionParser[T](programName: String, zero: T) extends scopt.OptionParser[T](programName) with LazyLogging {

  def version: String = BuildInfo(this.getClass).version

  head(programName, version)
  help('h', "help").text("show usage")
  version('v', "version")

  @throws[IllegalArgumentException]
  def parse(args: Seq[String]): T = {
    this.parse(args, zero).getOrElse(throw new IllegalArgumentException("Argument parsing failed"))
  }

  override def reportError(msg: String): Unit = {
    throw new IllegalArgumentException(msg)
  }

  override def reportWarning(msg: String): Unit = {
    logger.warn(msg)
  }

  override def showHeader(): Unit = {
    logger.info(header)
  }

  override def showUsage(): Unit = {
    logger.info(usage)
  }

  override def showUsageAsError(): Unit = {
    logger.error(usage)
  }

  override def showTryHelp(): Unit = {
    def oxford(xs: List[String]): String = xs match {
      case a :: b :: Nil => a + " or " + b
      case _ => (xs.dropRight(2) :+ xs.takeRight(2).mkString(", or ")).mkString(", ")
    }

    val str = oxford(helpOptions.toList.map(_.fullName))
    logger.error(s"Try $str for more information.")
  }
}

object OptionParser {
  implicit val durationRead: Read[Duration] = {
    implicitly[Read[duration.Duration]].map(dur => Duration.ofMillis(dur.toMillis))
  }

  implicit val pathRead: Read[Path] = implicitly[Read[String]].map(new Path(_))
}
