package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.netcdf.chunking.DataShape
import ucar.nc2.Variable

private[ucar] case class VariableWriterAccess(variable: Variable, shape: DataShape) extends UcarArrayBuilder