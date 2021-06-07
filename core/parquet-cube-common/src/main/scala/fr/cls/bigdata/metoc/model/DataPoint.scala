package fr.cls.bigdata.metoc.model

import fr.cls.bigdata.georef.model.VariableRef

/**
  * A datapoint in a Metoc source
  *
  * @param values The map of variables and its corresponding values. A None value means a fill value.
  *               All variable must have the same dimensions.
  */
final case class DataPoint(coordinates: Coordinates, values: Seq[(VariableRef, Option[Double])])
