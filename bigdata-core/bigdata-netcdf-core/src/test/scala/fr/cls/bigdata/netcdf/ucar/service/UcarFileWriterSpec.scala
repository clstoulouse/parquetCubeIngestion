package fr.cls.bigdata.netcdf.ucar.service

import fr.cls.bigdata.georef.metadata.DatasetMetadata
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.netcdf.{BaseUnitSpec, TestData}
import fr.cls.bigdata.resource.Resource

import scala.collection.convert.DecorateAsScala

class UcarFileWriterSpec extends BaseUnitSpec with TestData with DecorateAsScala {
  describe("write") {
    it("should write the dimension values") {
      for (fileWriter <- createFileWriter()) {
        fileWriter.writeDimension(Longitude.ref, Longitude.rawData)

        val file = fileWriter.file
        val variable = file.getVariables.asScala.find(v => v.getShortName == Longitude.shortName ).get
        val array = variable.read()

        array.getSize shouldBe Longitude.length.toLong
        for((value, index) <- Longitude.rawData.zipWithIndex) array.getObject(index) shouldBe value
      }
    }

    it ("should write the variable values") {
      for (fileWriter <- createFileWriter()) {
        fileWriter.writeVariable(Variable3D.ref, Variable3D.chunk, Variable3D.rawData)

        val file = fileWriter.file
        val variable = file.getVariables.asScala.find(v => v.getShortName == Variable3D.shortName).get
        val array = variable.read()

        array.getSize shouldBe Variable3D.rawData.size.toLong
        for ((value, index) <- Variable3D.rawData.zipWithIndex) array.getObject(index) shouldBe value
      }
    }

    it ("should fill the empty chunks with fill values") {
      for (fileWriter <- createFileWriter()) {
        val file = fileWriter.file
        val variable = file.getVariables.asScala.find(v => v.getShortName == Variable3D.shortName).get
        val array = variable.read()

        array.getSize shouldBe Variable3D.rawData.size.toLong
        for (index <- Variable3D.chunk.range) array.getObject(index) shouldBe Variable3D.fillValue
      }
    }
  }

  def createFileWriter(): Resource[UcarFileWriter] = {
    val folder = HadoopTestUtils.createTempDir()
    val path = folder.child("file.nc4").path
    val dimensions = Map(Longitude.ref -> Longitude.metadata, Latitude.ref -> Latitude.metadata, Time.ref -> Time.metadata)
    val metadata = DatasetMetadata(dimensions, Map(Variable3D.ref -> Variable3D.metadata), Set())
    val dimensionsLength = Map(Longitude.ref -> Longitude.length, Latitude.ref -> Latitude.length, Time.ref -> Time.length)
    UcarFileBuilder().create(metadata, dimensionsLength, path)
  }

}
