package fr.cls.bigdata.metoc.ingestion.model

import fr.cls.bigdata.hadoop.model.AbsoluteAndRelativePath
import fr.cls.bigdata.metoc.ingestion.config.DatasetReaderSettings
import fr.cls.bigdata.metoc.settings.DatasetSettings

case class CrawlingTask(settings: DatasetSettings, readerSettings: DatasetReaderSettings, file: AbsoluteAndRelativePath)
