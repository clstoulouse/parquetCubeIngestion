package fr.cls.bigdata.metoc.parquet.writer.services

import com.typesafe.scalalogging.LazyLogging
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.model.DataPoint
import fr.cls.bigdata.metoc.parquet.core.services.MetocAvroSchemaBuilder
import fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetWriterConfiguration
import fr.cls.bigdata.metoc.service.{MetocDatasetAccess, MetocWriter}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.avro.Schema

/**
  * Implementation of [[fr.cls.bigdata.metoc.service.MetocWriter]] that write metoc data to a parquet file structure, using a predefined partitioning strategy.
  *
  * @param schemaBuilder     Avro schema registry, to use to create/build the schema corresponding to the data to write.
  * @param config            Parquet writer configuration.
  * @param fileWriterFactory Parquet file writer factory.
  * @example
  * An output parquet files can look like this:
  * <table>
  * <tr>
  * <th>time</th>
  * <th>longitude</th>
  * <th>latitude</th>
  * <th>depth</th>
  * <th>var1</th>
  * <th>var2</th>
  * <th>...</th>
  * </tr>
  * <tr>
  * <td>`time1`</td>
  * <td>`lon1`</td>
  * <td>`lat1`</td>
  * <td>`depth1`</td>
  * <td>`val1`</td>
  * <td>`val2`</td>
  * <td>...</td>
  * </tr>
  * <tr>
  * <td>`time2`</td>
  * <td>`lon2`</td>
  * <td>`lat2`</td>
  * <td>`depth2`</td>
  * <td>`NULL`</td>
  * <td>`val2`</td>
  * <td>...</td>
  * </tr>
  * <tr>
  * <td>...</td>
  * </tr>
  * </table>
  * <br/>
  * @note
  * The written parquet verify the following statements:
  * <ul>
  * <li>Time dimension will be materialized in the schema by a column named [[fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants#TimeColumnName]] of type `long`.</li>
  * <li>Longitude dimension will be materialized in the schema by a column named [[fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants#LongitudeColumnName]] of type `double`.</li>
  * <li>Latitude dimension will be materialized in the schema by a column named [[fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants#LatitudeColumnName]] of type `double`.</li>
  * <li>If at least one variable have a depth dimension, the depth will be materialized in the schema by a column named [[fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants#DepthColumnName]] of type `double`.</li>
  * <li>Each variable will be materialized in the schema by a column named after the standard name of the variable, of type `double`.</li>
  * <li>Each row corresponds to a data point (coordinates + variable values).</li>
  * <li>The rows in the parquet files will be sorted in lexicographical ascending order on these columns: time, longitude, latitude, depth (if exist)</li>
  * <li>Fill and missing values are represented by `NULL` cells.</li>
  * <li>When the dataset have both 3d and 4d variables, the depth column will be optional. In this case in the parquet file:
  * <ul>
  * <li>The rows with `NULL` as depth will contain values for 3D variables and `NULL` for 4D variables.</li>
  * <li>The rows with NON-`NULL` as depth will contain values for 4D variables and `NULL` for 3D variables.</li>
  * </ul></li>
  *
  * </ul>
  */
class MetocParquetWriter(schemaBuilder: MetocAvroSchemaBuilder,
                         config: MetocParquetWriterConfiguration,
                         fileWriterFactory: MetocParquetFileWriterFactory) extends MetocWriter[Iterator[DataPoint]] with LazyLogging {

  @throws[MetocWriterException]
  override def write(datasetSettings: DatasetSettings, dataset: MetocDatasetAccess[Iterator[DataPoint]]): Unit = {
    val outputURL = datasetSettings.dataFolder

    logger.debug(s"writing data from metoc reader ${dataset.name} for dataset '${datasetSettings.name}' in the folder $outputURL with partitioning strategy " +
      s"${config.partitioningStrategy.name}")

    val schema = schemaBuilder.buildSchema(dataset.metadata.variables.keySet, dataset.geoRef)
    logger.debug(s"retrieved parquet schema: $schema")

    val partitioner = config.partitioningStrategy.partitioner(dataset.grid)

    val fileWriters = (for (partition <- partitioner.partitions) yield {
      partition -> openFileWriter(outputURL, partition.name, dataset.name, schema)
    }).toMap

    try {
      for (dataPoint <- dataset.data) {
        val fileWriter = fileWriters(partitioner.partition(dataPoint.coordinates))
        fileWriter.writeDataPoint(dataPoint.coordinates, dataPoint.values)
      }
    } finally {
      fileWriters.values.foreach(_.close())
    }
  }

  private def openFileWriter(outputURL: PathWithFileSystem, partitionName: String, fileNamePrefix: String, schema: Schema): MetocParquetFileWriter = {
    val filePath = getFullPath(outputURL, partitionName, fileNamePrefix)
    logger.debug(s"creating new parquet file writer for partition '$partitionName': $filePath")
    fileWriterFactory.open(filePath, schema, config.fileWriterConfiguration)
  }

  /**
    * @param folderName Name of the partition.
    * @return The full path to the parquet file corresponding to a partition.
    */
  private def getFullPath(outputURL: PathWithFileSystem, folderName: String, fileNamePrefix: String): PathWithFileSystem = {
    val fileName = s"$fileNamePrefix${config.fileWriterConfiguration.compressionCodec.getExtension}${MetocParquetConstants.ParquetFileExtension}"
    outputURL.child(folderName).child(fileName)
  }
}

object MetocParquetWriter {
  def apply(writerConfig: MetocParquetWriterConfiguration): MetocParquetWriter =
    new MetocParquetWriter(MetocAvroSchemaBuilder, writerConfig, MetocParquetFileWriter)
}
