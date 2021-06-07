package fr.cls.bigdata.metoc.ingestion.internal

import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.metoc.ingestion.TestData
import fr.cls.bigdata.netcdf.chunking.DataChunk
import fr.cls.bigdata.netcdf.service.NetCDFFileReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class LocalAccessSpec extends FunSpec with Matchers with MockFactory with TestData {
  describe("readAll") {
    it("should read 3D variables") {
      val fileReader = stub[NetCDFFileReader]
      (fileReader.read(_: VariableRef, _: DataChunk)).when(Variable3D.ref, Variable3D.chunk).returns(Variable3D.storage)
      val dataAccess = new LocalAccess(fileReader, Only3D.grid, Only3D.variables)

      dataAccess.get.toSeq should contain theSameElementsAs Only3D.dataPoints
    }

    it("should read 3D and 4D variables") {
      val file = stub[NetCDFFileReader]
      (file.read(_: VariableRef, _: DataChunk)).when(Variable3D.ref, Variable3D.chunk).returns(Variable3D.storage)
      (file.read(_: VariableRef, _: DataChunk)).when(Variable4D.ref, Variable4D.chunk).returns(Variable4D.storage)

      val dataAccess = new LocalAccess(file, Mixed3DAnd4D.grid, Mixed3DAnd4D.variables)

      dataAccess.get.toSeq should contain theSameElementsAs Mixed3DAnd4D.dataPoints
    }
  }
}
