package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.model.DimensionRef
import ucar.nc2.{Dimension, Variable}

private[ucar] case class DimensionWriterAccess(ref: DimensionRef,
                                               dimension: Dimension, length: Int,
                                               variable: Variable) extends UcarArrayBuilder
