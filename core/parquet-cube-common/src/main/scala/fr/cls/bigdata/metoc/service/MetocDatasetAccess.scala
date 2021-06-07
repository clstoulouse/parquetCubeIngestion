package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.metoc.model.{GeoReference, Grid}

/**
  * An access to a Metoc dataset, typically from a netcdf file, in the form of type A,
  * depending on how it is accessed. Type A could be an iterator of datapoints or a spark dataframe.
  *
  * @param name the name of the dataset access, typically the file name
  * @param metadata the metadata associated with the dataset
  * @param grid the grid of coordinates of this particular dataset
  * @param access the access to the data
  * @tparam A the type of the data
  */
case class MetocDatasetAccess[+A](name: String, metadata: DatasetMetadata, grid: Grid, access: DataAccess[A]) {
  def geoRef: GeoReference = {
    val shapes = metadata.variables.values.map(_.dimensions.size)
    if (shapes.forall(_ == 3)) GeoReference.Only3D
    else if (shapes.forall(_ == 4)) GeoReference.Only4D
    else if (shapes.forall(d => d == 4 || d == 3)) GeoReference.Mixed3DAnd4D
    else throw new IllegalStateException(s"invalid dimensions in dataset $name: ${metadata.dimensions.keySet}")
  }

  def data: A = access.get
}

trait DataAccess[+A] {
  def get: A
}
