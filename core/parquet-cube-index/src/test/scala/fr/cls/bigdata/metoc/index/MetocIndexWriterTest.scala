package fr.cls.bigdata.metoc.index

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.model.Grid
import fr.cls.bigdata.metoc.service.{DataAccess, MetocDatasetAccess}
import fr.cls.bigdata.metoc.settings.DatasetSettings
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

import scala.collection.SortedSet

class MetocIndexWriterTest extends FunSpec with MockFactory with Matchers with HadoopTestUtils {
  private val mockAccess = stub[DataAccess[Any]]
  private val datasetName = "dataset1"
  private val grid1 = Grid(time = SortedSet(1L, 2L), longitude = SortedSet(3D, 4D), latitude = SortedSet(5D, 6D), depth = SortedSet(7D, 9D))
  private val grid2 = Grid(time = SortedSet(10L, 11L), longitude = SortedSet(12D, 13D), latitude = SortedSet(14D, 15D), depth = SortedSet())

  describe("write") {
    it("should write each dimension index once when grid.depth is not empty") {
      //prepare
      val datasetSettings = createTempDatasetSettings()
      val dataset = MetocDatasetAccess(datasetName, DatasetMetadata(), grid1, mockAccess)
      val service = mock[MetocIndexService]
      val writer = new MetocIndexWriter(service, HadoopConfiguration)

      //expectation
      inAnyOrder {
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.longitudeFile(datasetSettings.indexFolder), grid1.longitude).once()
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.latitudeFile(datasetSettings.indexFolder), grid1.latitude).once()
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.depthFile(datasetSettings.indexFolder), grid1.depth).once()
        (service.writeMutableDimensionIndex _)
          .expects(MetocIndex.timeFile(datasetSettings.indexFolder), grid1.time).once()
      }

      writer.write(datasetSettings, dataset)
    }

    it("should ignore depth when grid.depth is empty") {
      //prepare
      val datasetSettings = createTempDatasetSettings()
      val dataset = MetocDatasetAccess(datasetName, DatasetMetadata(), grid2, mockAccess)
      val service = mock[MetocIndexService]
      val writer = new MetocIndexWriter(service, HadoopTestUtils.HadoopConfiguration)

      //expectation
      inAnyOrder {
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.longitudeFile(datasetSettings.indexFolder), grid2.longitude).once()
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.latitudeFile(datasetSettings.indexFolder), grid2.latitude).once()
        (service.writeImmutableDimensionIndex _)
          .expects(MetocIndex.depthFile(datasetSettings.indexFolder), grid2.depth).never()
        (service.writeMutableDimensionIndex _)
          .expects(MetocIndex.timeFile(datasetSettings.indexFolder), grid2.time).once()
      }

      writer.write(datasetSettings, dataset)
    }
  }

  private def createTempDatasetSettings(): DatasetSettings = {
    DatasetSettings(
      name = "dataset1",
      dataFolder = PathWithFileSystem("data-path", HadoopConfiguration),
      indexFolder = HadoopTestUtils.createTempDir()
    )
  }
}
