package fr.cls.bigdata.metoc.impl

import fr.cls.bigdata.georef.model.Bounds
import fr.cls.bigdata.metoc.mock.GridServiceMock
import fr.cls.bigdata.metoc.model.Coordinates
import fr.cls.bigdata.metoc.service.MetocGridService
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}

class TimeShiftedGridServiceSpec extends FunSpec with Matchers with MockFactory {

  describe("find3DNeighbors") {
    it("should shift the neighbors") {
      val innerGridService = new GridServiceMock(Map(
        Coordinates(1D, 2D, 50L, None) -> Seq(Coordinates(1D, 2D, 40L, None))
      ))
      val gridService = TimeShiftedGridService(innerGridService, 50L)
      gridService.findGeoNeighbors(Coordinates(1D, 2D, 100L, None)) shouldBe Seq(Coordinates(1D, 2D, 90L, None))
    }
  }

  describe("intesectWith") {
    it("should shift the bounds intersection") {
      val innerGridService = stub[MetocGridService]
      (innerGridService.findClosure _)
        .when(Bounds(1D, 10D, 2D, 20D, 50L, 150L))
        .returns(Bounds(1D, 10D, 2D, 20D, 40L, 160L))

      val gridService = TimeShiftedGridService(innerGridService, 50L)

      val bounds = Bounds(1D, 10D, 2D, 20D, 100L, 200L)
      gridService.findClosure(bounds) shouldBe Bounds(1D, 10D, 2D, 20D, 90L, 210L)
    }
  }
}
