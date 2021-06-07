package fr.cls.bigdata.metoc.metadata

import org.scalatest.{FunSpec, Matchers}
import spray.json.{DeserializationException, JsObject, JsString}

class MetadataJsonFormatSpec extends FunSpec with Matchers with UnitTestData {

  describe("read") {
    it("should convert json into metadata attribute") {
      MetadataJsonFormat.readAttribute(Attribute.name, Attribute.json) shouldBe Attribute.metadata
    }

    it("should convert json into dimension metadata") {
      MetadataJsonFormat.readDimension(Longitude.json) shouldBe Longitude.metadata
    }

    it("should convert json into variable metadata") {
      MetadataJsonFormat.readVariable(Variable3D.json) shouldBe Variable3D.metadata
    }

    it("should convert json into dataset metadata") {
      MetadataJsonFormat.read(Dataset.json) shouldBe Dataset.metadata
    }

    it(s"should throw an ${classOf[DeserializationException].getSimpleName} when invalid json") {
      a[DeserializationException] shouldBe thrownBy(MetadataJsonFormat.read(JsObject("invalid" -> JsString("json"))))
    }
  }

  describe("write") {
    it("should convert metadata attribute to Json") {
      MetadataJsonFormat.write(Attribute.metadata) shouldBe Attribute.json
    }

    it("should convert dimension metadata to Json") {
      MetadataJsonFormat.write(Longitude.metadata) shouldBe Longitude.json
    }

    it("should convert variable metadata to Json") {
      MetadataJsonFormat.write(Variable3D.metadata) shouldBe Variable3D.json
    }

    it("should convert dataset metadata to Json") {
      MetadataJsonFormat.write(Dataset.metadata) shouldBe Dataset.json
    }
  }

}
