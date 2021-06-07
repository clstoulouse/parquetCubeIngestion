package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.ConfigException.BadValue

sealed trait IngestionMode

object IngestionMode {
  case object Local extends IngestionMode
  case object Spark extends IngestionMode

  def fromName(name: String): IngestionMode = name match {
    case "local" => Local
    case "spark" => Spark
    case _ => throw new BadValue("", s"invalid ingestion mode $name")
  }
}
