package fr.cls.bigdata.metoc.metadata.diff

/**
  * Side of a diff operation.
  */
sealed trait DiffSide

object DiffSide {

  /**
    * Left side of a diff operation.
    */
  case object Left extends DiffSide

  /**
    * Right side of a diff operation.
    */
  case object Right extends DiffSide

}