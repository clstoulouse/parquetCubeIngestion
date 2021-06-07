package fr.cls.bigdata.metoc.parquet.writer.services

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.model._
import fr.cls.bigdata.metoc.parquet.core.services.MetocAvroSchemaBuilder
import fr.cls.bigdata.metoc.parquet.core.utils.MetocParquetConstants
import fr.cls.bigdata.metoc.parquet.utils.MetocParquetTestUtils
import fr.cls.bigdata.metoc.parquet.writer.objs.{MetocParquetFileWriterConfiguration, MetocParquetWriterConfiguration}
import fr.cls.bigdata.metoc.parquet.writer.partition.{Partition, PartitioningStrategy}
import fr.cls.bigdata.metoc.service.{DataAccess, MetocDatasetAccess}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.apache.avro.Schema
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

import scala.collection.SortedSet

class MetocParquetWriterTest extends FunSpec with Matchers with MockFactory with MetocParquetTestUtils with HadoopTestUtils {

  private final val datasetName = "dataset1"
  private final val datasetSettings = DatasetSettings(
    datasetName,
    dataFolder = PathWithFileSystem("data-path", HadoopConfiguration),
    indexFolder = PathWithFileSystem("other-path", HadoopConfiguration)
  )

  describe("MetocParquetWriter.write") {

    it("should create one parquet file per partition") {
      // prepare
      val partition1 = Partition("part1")
      val partition2 = Partition("part2")
      val partitioningStrategy = PartitioningStrategyMock(partition1 -> Seq(), partition2 -> Seq())

      // init stubs/mocks
      val writerConfiguration = newWriterConfigurationStub(partitioningStrategy)
      val schemaBuilder: MetocAvroSchemaBuilder = newSchemaBuilderStub(variable3Ds, GeoReference.Only3D, AvroSchema3dOnly)
      val dataset = createDatasetAccess(variable3DsMetadata, Iterator())

      val parquetFileWriterFactory = mock[MetocParquetFileWriterFactory]
      val fileWriter1 = mock[MetocParquetFileWriter]
      val fileWriter2 = mock[MetocParquetFileWriter]

      // expectations
      inSequence {
        (parquetFileWriterFactory.open _)
          .expects(
            datasetSettings.dataFolder.child("part1").child(s"$datasetName.snappy${MetocParquetConstants.ParquetFileExtension}"),
            AvroSchema3dOnly,
            writerConfiguration.fileWriterConfiguration)
          .returns(fileWriter1)
        (fileWriter1.close _).expects().returns()
      }

      inSequence {
        (parquetFileWriterFactory.open _)
          .expects(
            datasetSettings.dataFolder.child("part2").child(s"$datasetName.snappy${MetocParquetConstants.ParquetFileExtension}"),
            AvroSchema3dOnly,
            writerConfiguration.fileWriterConfiguration)
          .returns(fileWriter2)
      }

      (fileWriter2.close _).expects().returns()

      // perform
      val parquetWriter = new MetocParquetWriter(schemaBuilder, writerConfiguration, parquetFileWriterFactory)
      parquetWriter.write(datasetSettings, dataset)
    }

    it("should write as many data points as there are in a 3d-only grid") {
      // prepare

      val coordinates = for {
        time <- Seq(Time1, Time2)
        longitude <- Seq(Lon1, Lon2)
        latitude <- Seq(Lat1, Lat2)
      } yield Coordinates(longitude, latitude, time, None)

      val dataPoints = for {
        (coordinates, i) <- coordinates.zipWithIndex
      } yield DataPoint(coordinates, Seq(Var3d_1 -> Some(1D + i), Var3d_2 -> Some(2D + i)))

      val partition = Partition("part")
      val partitioningStrategy = PartitioningStrategyMock(partition -> coordinates)

      // init stubs/mocks
      val writerConfiguration = newWriterConfigurationStub(partitioningStrategy)
      val schemaBuilder = newSchemaBuilderStub(variable3Ds, GeoReference.Only3D, AvroSchema3dOnly)
      val dataset = createDatasetAccess(variable3DsMetadata, dataPoints.toIterator)

      val parquetFileWriterFactory = mock[MetocParquetFileWriterFactory]
      val fileWriter = mock[MetocParquetFileWriter]

      // expectations
      inSequence {
        (parquetFileWriterFactory.open _)
          .expects(
            datasetSettings.dataFolder.child("part").child(s"$datasetName.snappy${MetocParquetConstants.ParquetFileExtension}"),
            AvroSchema3dOnly,
            writerConfiguration.fileWriterConfiguration)
          .returns(fileWriter)

        for (DataPoint(coordinates, values) <- dataPoints) {
          (fileWriter.writeDataPoint _).expects(coordinates, values).returns()
        }

        (fileWriter.close _).expects().returns()
      }

      // perform
      val parquetWriter = new MetocParquetWriter(schemaBuilder, writerConfiguration, parquetFileWriterFactory)
      parquetWriter.write(datasetSettings, dataset)
    }

    it("should write as many data points as there are in a 4d-only grid") {
      // prepare
      val coordinates = for {
        time <- Seq(Time1, Time2)
        longitude <- Seq(Lon1, Lon2)
        latitude <- Seq(Lat1, Lat2)
        depth <- Seq(Depth1, Depth2)
      } yield Coordinates(longitude, latitude, time, Some(depth))

      val dataPoints = for {
        (coordinates, i) <- coordinates.zipWithIndex
      } yield DataPoint(coordinates, Seq(Var4d_1 -> Some(1D + i), Var4d_2 -> Some(2D + i)))

      val partition = Partition("part")
      val partitioningStrategy = PartitioningStrategyMock(partition -> coordinates)

      // init stubs/mocks
      val writerConfiguration = newWriterConfigurationStub(partitioningStrategy)
      val schemaBuilder: MetocAvroSchemaBuilder = newSchemaBuilderStub(variable4Ds, GeoReference.Only4D, AvroSchema4dOnly)
      val dataset = createDatasetAccess(variable4DsMetadata, dataPoints.toIterator)

      val parquetFileWriterFactory = mock[MetocParquetFileWriterFactory]
      val fileWriter = mock[MetocParquetFileWriter]

      // expectations
      inSequence {
        (parquetFileWriterFactory.open _)
          .expects(
            datasetSettings.dataFolder.child("part").child(s"$datasetName.snappy${MetocParquetConstants.ParquetFileExtension}"),
            AvroSchema4dOnly,
            writerConfiguration.fileWriterConfiguration)
          .returns(fileWriter)

        for (DataPoint(coordinates, values) <- dataPoints) {
          (fileWriter.writeDataPoint _).expects(coordinates, values).returns()
        }

        (fileWriter.close _).expects().returns()
      }

      // perform
      val parquetWriter = new MetocParquetWriter(schemaBuilder, writerConfiguration, parquetFileWriterFactory)
      parquetWriter.write(datasetSettings, dataset)
    }

    it("should write as many data points as there are in a mixed 3d/4d grid") {
      // prepare
      val coordinates = for {
        time <- Seq(Time1, Time2)
        longitude <- Seq(Lon1, Lon2)
        latitude <- Seq(Lat1, Lat2)
        depth <- Seq[Option[Double]](Some(Depth1), Some(Depth2), None)
      } yield Coordinates(longitude, latitude, time, depth)

      val dataPoints = for {
        (coordinates, i) <- coordinates.zipWithIndex
      } yield DataPoint(
        coordinates,
        Seq(
          Var3d_1 -> Some(1D + i),
          Var3d_2 -> Some(2D + i),
          Var4d_1 -> Some(3D + i),
          Var4d_2 -> Some(4D + i)
        )
      )


      val partition = Partition("part")
      val partitioningStrategy = PartitioningStrategyMock(partition -> coordinates)

      // init stubs/mocks
      val writerConfiguration = newWriterConfigurationStub(partitioningStrategy)
      val schemaBuilder: MetocAvroSchemaBuilder = newSchemaBuilderStub(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D, AvroSchemaMixed3dAnd4d)
      val dataset = createDatasetAccess(mixedMetadata, dataPoints.toIterator)

      val parquetFileWriterFactory = mock[MetocParquetFileWriterFactory]
      val fileWriter = mock[MetocParquetFileWriter]

      //expectations
      inSequence {
        (parquetFileWriterFactory.open _)
          .expects(
            datasetSettings.dataFolder.child("part").child(s"$datasetName.snappy${MetocParquetConstants.ParquetFileExtension}"),
            AvroSchemaMixed3dAnd4d,
            writerConfiguration.fileWriterConfiguration)
          .returns(fileWriter)

        for (DataPoint(coordinates, values) <- dataPoints) {
          (fileWriter.writeDataPoint _).expects(coordinates, values).returns()
        }

        (fileWriter.close _).expects().returns()
      }

      // perform
      val parquetWriter = new MetocParquetWriter(schemaBuilder, writerConfiguration, parquetFileWriterFactory)
      parquetWriter.write(datasetSettings, dataset)
    }
  }

  private val fileWriterConfiguration = MetocParquetFileWriterConfiguration(CompressionCodecName.SNAPPY, HadoopTestUtils.HadoopConfiguration)

  private def newWriterConfigurationStub(partitioningStrategy: PartitioningStrategy): MetocParquetWriterConfiguration =
    MetocParquetWriterConfiguration(partitioningStrategy, fileWriterConfiguration)


  private def newSchemaBuilderStub(variables: Set[VariableRef], reference: GeoReference, schema: Schema): MetocAvroSchemaBuilder = {
    val schemaRegistry = stub[MetocAvroSchemaBuilder]
    (schemaRegistry.buildSchema _).when(variables, reference).returns(schema)
    schemaRegistry
  }

  private def createDatasetAccess(metadata: DatasetMetadata, dataPoints: Iterator[DataPoint]): MetocDatasetAccess[Iterator[DataPoint]] = {
    val dataAccess = stub[DataAccess[Iterator[DataPoint]]]
    (dataAccess.get _).when().returns(dataPoints)

    val grid = Grid(SortedSet.empty, SortedSet.empty, SortedSet.empty, SortedSet.empty)

    MetocDatasetAccess(datasetName, metadata, grid, dataAccess)
  }
}
