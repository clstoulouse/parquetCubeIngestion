package fr.cls.bigdata.metoc.ingestion.service

import fr.cls.bigdata.georef.model.{DataType, DimensionRef}
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.metoc.ingestion.TestData
import fr.cls.bigdata.netcdf.service.NetCDFFileReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class NetCDFLocalReaderSpec extends FunSpec with Matchers with MockFactory with TestData with HadoopTestUtils {
  describe("read") {
    it("should init metadata") {
      val netCDFFile = stub[NetCDFFileReader]

      (netCDFFile.file _).when().returns(toHadoopPath("file:///netcdf/dataset-alti8-nrt-global-msla-h.20190115.nc"))
      (netCDFFile.dimensions _).when().returns(Mixed3DAnd4D.netCDFDimensions)
      (netCDFFile.variables _).when().returns(Mixed3DAnd4D.variables)
      (netCDFFile.globalAttributes _).when().returns(globalAttributes)
      (netCDFFile.read(_: DimensionRef)).when(*).returns(DataType.Double.store(Seq.empty))

      val metadata = NetCDFLocalReader.read(netCDFFile, defaultRounding).metadata

      metadata.dimensions.keySet should contain theSameElementsAs Mixed3DAnd4D.dimensions
      metadata.variables.keySet should contain theSameElementsAs Set(Variable3D.ref, Variable4D.ref)

      metadata.attributes should contain theSameElementsAs globalAttributes
    }
  }
}
