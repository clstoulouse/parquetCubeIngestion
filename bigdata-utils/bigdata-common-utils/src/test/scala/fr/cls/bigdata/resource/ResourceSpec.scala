package fr.cls.bigdata.resource

import java.io.IOException

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class ResourceSpec extends FunSpec with Matchers with MockFactory {

  class SafeResource(value: Int) extends Resource[Int] {
    def close(): Unit = ()
    def get: Int = value
  }

  describe("sequence") {
    describe("get") {
      it("should get the inner values") {
        val resource1 = mock[Resource[Int]]
        val resource2 = mock[Resource[Int]]

        (resource1.get _).expects().returns(2).once()
        (resource2.get _).expects().returns(3).once()

        Resource.sequence(Seq(resource1, resource2)).get shouldBe Seq(2, 3)
      }
    }

    describe("close") {
      it("should close all the inner resource") {

        val resource1 = mock[Resource[Int]]
        val resource2 = mock[Resource[Int]]

        (resource1.close _).expects().once()
        (resource2.close _).expects().once()

        Resource.sequence(Seq(resource1, resource2)).close()
      }
    }

  }


  describe("map") {
    describe("get") {
      it("should map the inner value") {
        val resource = mock[Resource[Int]]
        (resource.get _).expects().returns(2).once()

        resource.map(_ * 2).get shouldBe 4
      }
    }

    describe("close") {
      it("should close the resource") {
        val resource = mock[Resource[Int]]

        inSequence {
          (resource.get _).expects().returns(2).once()
          (resource.close _).expects().once()
        }

        resource.map(_ * 2).close()
      }
    }
  }

  describe("flatMap") {
    describe("get") {
      it("should compute the inner resource with the outer value") {
        def mockResource(value: Int) = {
          val resource = mock[Resource[Int]]
          (resource.get _).expects().returns(value)
          resource
        }

        val outerResource = mockResource(2)

        outerResource.flatMap(value => mockResource(value * 2)).get shouldBe 4
      }
    }

    describe("close") {
      it("should close the inner resource first and then the outer resource") {
        val outerResource = mock[Resource[String]]
        (outerResource.get _).expects().returns("value")

        val innerResource = mock[Resource[Int]]

        inSequence {
          (innerResource.close _).expects().once()
          (outerResource.close _).expects().once()
        }

        outerResource.flatMap(_ => innerResource).close()
      }

      it("should close the outer resource even if the inner resource throws an exception") {
        val outerResource = mock[Resource[String]]
        (outerResource.get _).expects().returns("value")

        val innerResource = mock[Resource[Int]]

        inSequence {
          (innerResource.close _).expects().once().throws(new IOException())
          (outerResource.close _).expects().once()
        }

        an[IOException] should be thrownBy outerResource.flatMap(_ => innerResource).close()
      }
    }
  }

  describe("foreach") {
    it("should process the value and then close the resource") {
      val resource = mock[Resource[String]]
      (resource.get _).expects().returns("value")
      (resource.close _).expects().once()

      resource.foreach(value => value shouldBe "value")
    }

    it("should close the resource when an exception is thronw") {
      val resource = mock[Resource[String]]
      (resource.get _).expects().returns("value")
      (resource.close _).expects().once()

      an[IOException] should be thrownBy resource.foreach(_ => throw new IOException())
    }
  }

  describe("safeGet") {
    it("should retrieve the value and then close the resource") {
      val resource = mock[Resource[String]]
      inSequence {
        (resource.get _).expects().returns("value")
        (resource.close _).expects().once()
      }

      resource.safeGet shouldBe "value"
    }
  }

}
