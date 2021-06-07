package fr.cls.bigdata.metoc.ingestion.logging

object MdcKeys {
  final val DatasetName = "dataset-name"
  final val InputFile = "crawler.input-file"
  final val LifecycleEventType = "crawler.event-type"
  final val CompletionType = "crawler.completion-type"
  final val ExecutionTime = "crawler.execution-time-millis"
}
