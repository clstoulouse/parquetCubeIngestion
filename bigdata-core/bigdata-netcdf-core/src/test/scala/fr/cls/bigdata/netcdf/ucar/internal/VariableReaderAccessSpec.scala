package fr.cls.bigdata.netcdf.ucar.internal

import fr.cls.bigdata.georef.metadata.{MetadataAttribute, VariableMetadata}
import fr.cls.bigdata.georef.model.{DataType, VariableRef}
import fr.cls.bigdata.netcdf.BaseUnitSpec
import fr.cls.bigdata.netcdf.chunking.DataShape

class VariableReaderAccessSpec extends BaseUnitSpec {
  import fr.cls.bigdata.georef.metadata.Constants._

  private val shape = DataShape(Seq())
  private val standardName = "standardName"
  private val shortName = "shortName"
  private val longName = "longName"

  describe("ref") {
    it("should choose standard name when it is not empty") {
      val attributes = Set(MetadataAttribute(StandardNameAttribute, DataType.String, Seq(standardName)))
      val metadata = VariableMetadata(shortName, DataType.Double, Seq(), attributes)
      val variableAccess = VariableReaderAccess(metadata, shape, null)

      variableAccess.ref shouldBe VariableRef(standardName)
    }

    it("should choose long name when standard name is empty") {
      val attributes = Set(
        MetadataAttribute(StandardNameAttribute, DataType.String, Seq("")),
        MetadataAttribute(LongNameAttribute, DataType.String, Seq(longName))
      )
      val metadata = VariableMetadata(shortName, DataType.Double, Seq(), attributes)
      val variableAccess = VariableReaderAccess(metadata, shape, null)

      variableAccess.ref shouldBe VariableRef(longName)
    }

    it("should choose short name when standard name and long name are empty") {
      val attributes = Set(
        MetadataAttribute(StandardNameAttribute, DataType.String, Seq("")),
        MetadataAttribute(LongNameAttribute, DataType.String, Seq(""))
      )
      val metadata = VariableMetadata(shortName, DataType.Double, Seq(), attributes)
      val variableAccess = VariableReaderAccess(metadata, shape, null)

      variableAccess.ref shouldBe VariableRef(shortName)
    }
  }

}
