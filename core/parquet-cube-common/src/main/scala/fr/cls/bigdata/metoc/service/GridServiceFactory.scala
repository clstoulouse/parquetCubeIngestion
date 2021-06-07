package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.settings.DatasetSettings

trait GridServiceFactory {

  /**
    * Creates a [[fr.cls.bigdata.metoc.service.MetocGridService]]
    *
    * @param datasetSettings the settings of a dataset
    * @throws MetocReaderException when an error occurs while reading the grid
    * @return a service to
    */
  @throws[MetocReaderException]
  def create(datasetSettings: DatasetSettings): MetocGridService
}
