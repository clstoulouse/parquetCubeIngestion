package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.metoc.exceptions.{MetocReaderException, MetocWriterException}
import fr.cls.bigdata.metoc.settings.DatasetSettings

/**
  * Base interface that writes metoc data to a storage.
  */
trait MetocWriter[-A] {

  /**
    * Writes the data from a reader to a storage.
    *
    * @param dataset A dataset access that provides the data in the form of A.
    * @throws MetocWriterException In case of error when writing the data points.
    */
  @throws[MetocWriterException]
  @throws[MetocReaderException]
  def write(datasetSettings: DatasetSettings, dataset: MetocDatasetAccess[A]): Unit
}

object MetocWriter {

  /**
    * Creates a composite writer that delegates its tasks sequentially to multiple underlying concrete writers.
    *
    * @param writers writers to delegate to.
    * @return Composite writer instance.
    */
  def composite[A](writers: MetocWriter[A]*): MetocWriter[A] = new CompositeWriter(writers)

  private class CompositeWriter[A](writers: Seq[MetocWriter[A]]) extends MetocWriter[A] {
    override def write(datasetSettings: DatasetSettings, dataset: MetocDatasetAccess[A]): Unit = {
      writers.foreach { writer =>
        writer.write(datasetSettings, dataset)
      }
    }
  }

}
