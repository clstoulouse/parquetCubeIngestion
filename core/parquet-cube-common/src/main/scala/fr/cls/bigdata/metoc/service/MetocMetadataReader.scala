package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.metoc.exceptions.MetocReaderException
import fr.cls.bigdata.metoc.settings.DatasetSettings

trait MetocMetadataReader {
  /**
    * @param datasetSettings the settings of a metoc dataset
    * @throws MetocReaderException when an error occur while accessing the metadata
    * @return the metadata of this dataset
    */
  @throws[MetocReaderException]
  def readAll(datasetSettings: DatasetSettings): DatasetMetadata

  /**
    * read the metadata of the dataset for a subset of variables
    *
    * @param datasetSettings the settings of a metoc dataset
    * @param variables a subset of the dataset's variables
    * @throws MetocReaderException when an error occur while accessing the metadata
    * @return the metadata of this dataset for the subset of variables
    */
  @throws[MetocReaderException]
  def read(datasetSettings: DatasetSettings, variables: Seq[VariableRef]): DatasetMetadata
}
