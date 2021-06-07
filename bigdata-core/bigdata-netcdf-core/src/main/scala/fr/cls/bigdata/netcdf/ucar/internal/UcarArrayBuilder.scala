package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.model.DataStorage
import ucar.ma2.{Array => UcarArray}

private[ucar] trait UcarArrayBuilder {
  def buildArray(data: DataStorage, shape: Array[Int]): UcarArray = {
    UcarArray.factory(UcarMapper.toUcarType(data.dataType), shape, data.storage)
  }
}
