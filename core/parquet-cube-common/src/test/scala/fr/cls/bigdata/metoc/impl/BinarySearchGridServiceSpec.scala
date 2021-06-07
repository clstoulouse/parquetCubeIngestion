package fr.cls.bigdata.metoc.impl

import fr.cls.bigdata.georef.model.Bounds
import fr.cls.bigdata.georef.model.ranges.{LatitudeRange, LongitudeRange, TimeRange}
import fr.cls.bigdata.metoc.model.{Coordinates, Grid}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers, OptionValues}

class BinarySearchGridServiceSpec extends FunSpec with TableDrivenPropertyChecks with Matchers with OptionValues {

  private val grid = Grid.create(
    longitude = (-10D) to (10D, step = 1D),
    latitude = 20D to (60D, step = 2D),
    time = 80L to (140L, step = 3L),
    depth = Seq(1D, 2D, 3D)
  )

  private val defaultCoordinates = Coordinates(0.5D, 30.5D, 110L, None)
  private val gridService = new BinarySearchGridService(grid)

  describe("findBounds") {

    it("should return the closure of the value") {
      val cases = Table(
        ("values", "value", "bounds"),
        (Seq(1D, 2D, 3D), 2.5D, (Some(2D), Some(3D))),
        (Seq(1D, 2D, 3D), 0.5D, (None, Some(1D))),
        (Seq(1D, 2D, 3D), 3.5D, (Some(3D), None)),
        (Seq(1D, 2D, 3D), 1D, (Some(1D), Some(1D)))
      )

      forAll(cases) { (values, value, bounds) =>
        BinarySearchGridService.findClosure(values, value) shouldBe bounds
      }
    }
  }

  describe("find3DNeighbors") {
    it("should return nothing if time is out of the grid") {
      val times = Table("time", 0L, 79L, 141L, 1000L)
      forAll(times) { time =>
        val coordinates = defaultCoordinates.copy(time = time)
        gridService.findGeoNeighbors(coordinates) shouldBe empty
      }
    }

    it("should return nothing if longitude is out of the grid") {
      val longitudes = Table("longitude", -100D, -10.001D, 11D, 1000D)
      forAll(longitudes) { longitude =>
        val coordinates = defaultCoordinates.copy(longitude = longitude)
        gridService.findGeoNeighbors(coordinates) shouldBe empty
      }
    }

    it("should return nothing if latitude is out of the grid") {
      val latitudes = Table("latitude", 0.5D, 19.99D, 60.01D, 1000D)
      forAll(latitudes) { latitude =>
        val coordinates = defaultCoordinates.copy(latitude = latitude)
        gridService.findGeoNeighbors(coordinates) shouldBe empty
      }
    }

    it("should return time if time matches the grid") {
      val time = 128L
      val coordinates = defaultCoordinates.copy(time = time)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 4
      neighbors.map(_.time) should contain only time
    }

    it("should return nearest time if time is inside the grid") {
      val time = 103L
      val coordinates = defaultCoordinates.copy(time = time)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 4
      neighbors.map(_.time) should contain only 104L
    }

    it("should return longitude if longitude matches the grid") {
      val longitude = 6D
      val coordinates = defaultCoordinates.copy(longitude = longitude)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 2
      neighbors.map(_.longitude) should contain only longitude
    }

    it("should return the two nearest longitudes if longitude is inside the grid") {
      val longitude = -2.5D
      val coordinates = defaultCoordinates.copy(longitude = longitude)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 4
      neighbors.map(_.longitude) should contain only(-3D, -2D)
    }

    it("should return latitude if latitude matches the grid") {
      val latitude = 32D
      val coordinates = defaultCoordinates.copy(latitude = latitude)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 2
      neighbors.map(_.latitude) should contain only latitude
    }

    it("should return the two nearest latitudes if latitude is inside the grid") {
      val latitude = 36.1D
      val coordinates = defaultCoordinates.copy(latitude = latitude)

      val neighbors = gridService.findGeoNeighbors(coordinates)

      neighbors.size shouldBe 4
      neighbors.map(_.latitude) should contain only(36D, 38D)
    }

    it("should return 3D points only ie no depth") {
      val coordinates = Table("coordinates",
        defaultCoordinates,
        defaultCoordinates.copy(depth = Some(2D))
      )
      forAll(coordinates) { coordinates =>
        val neighbors = gridService.findGeoNeighbors(coordinates)

        neighbors.size shouldBe 4
        neighbors.map(_.depth) should contain only None
      }
    }
  }

  describe("findClosure") {
    it("should return the original box when it matches the grid") {
      val bounds = Bounds(
        minLon = 4D,
        maxLon = 9D,
        minLat = 34D,
        maxLat = 38D,
        minTime = 98L,
        maxTime = 107L
      )
      val closure = gridService.findClosure(bounds)
      closure shouldBe bounds
    }

    it("should return the closure of the box when it is inside the grid") {
      val bounds = Bounds(
        minLon = 4.5,
        maxLon = 8.3D,
        minLat = 33.5D,
        maxLat = 37D,
        minTime = 97L,
        maxTime = 108L
      )
      val closure = gridService.findClosure(bounds)
      closure shouldBe Bounds(
        minLon = 4D,
        maxLon = 9D,
        minLat = 32D,
        maxLat = 38D,
        minTime = 95L,
        maxTime = 110L
      )
    }

    it("should return an empty bounds when it is outside the grid") {
      val bounds = Bounds(
        minLon = 10D,
        maxLon = 12D,
        minLat = 62D,
        maxLat = 68D,
        minTime = 200L,
        maxTime = 300L
      )
      val closure = gridService.findClosure(bounds)
      closure shouldBe Bounds.`3D`(LongitudeRange.inclusive(10D, 10D), LatitudeRange.Empty, TimeRange.Empty)
    }

    it("should return the closure of the box when it overlaps the grid") {
      val bounds = Bounds(
        minLon = 7D,
        maxLon = 12D,
        minLat = 16D,
        maxLat = 41D,
        minTime = 106L,
        maxTime = 300L
      )
      val closure = gridService.findClosure(bounds)
      closure shouldBe Bounds(
        minLon = 7D,
        maxLon = 10D,
        minLat = 20D,
        maxLat = 42D,
        minTime = 104L,
        maxTime = 140L
      )
    }
  }

  describe("intersection") {
    it("should return an empty grid when the box is outside the grid") {
      val bounds = Bounds(
        minLon = 11D,
        maxLon = 12D,
        minLat = 62D,
        maxLat = 68D,
        minTime = 200L,
        maxTime = 300L
      )
      val intersection = gridService.intersection(bounds)
      intersection shouldBe Grid.empty.copy(depth = grid.depth)
    }

    it("should return the intersection when the box is inside the grid") {
      val bounds = Bounds(
        minLon = 4.5D,
        maxLon = 8.3D,
        minLat = 33.5D,
        maxLat = 37D,
        minTime = 97L,
        maxTime = 108L
      )
      val intersection = gridService.intersection(bounds)
      intersection shouldBe Grid.create(
        longitude = 5D to (8D, step = 1D),
        latitude = 34D to (36D, step = 2D),
        time = 98L to (107L, step = 3L),
        depth = Seq(1D, 2D, 3D)
      )
    }

    it("should return the intersection when the box overlap the grid") {
      val bounds = Bounds(
        minLon = 7D,
        maxLon = 12D,
        minLat = 16D,
        maxLat = 41D,
        minTime = 106L,
        maxTime = 300L
      )
      val intersection = gridService.intersection(bounds)
      intersection shouldBe Grid.create(
        longitude = 7D to (10D, step = 1D),
        latitude = 20D to (40D, step = 2D),
        time = 107L to (140L, step = 3L),
        depth = Seq(1D, 2D, 3D)
      )
    }

    it("should return the intersection with an anti-meridian range") {
      val bounds = Bounds(
        LongitudeRange.AntiMeridianInclusive(7.5D, 351.5D),
        LatitudeRange.Inclusive(33.5D, 37D),
        TimeRange.Inclusive(97L, 108L)
      )
      val intersection = gridService.intersection(bounds)
      intersection shouldBe Grid.create(
        longitude = (-10D to (-9D, step = 1D)) ++ (8D to (10D, step = 1D)),
        latitude = 34D to (36D, step = 2D),
        time = 98L to (107L, step = 3L),
        depth = Seq(1D, 2D, 3D)
      )
    }
  }
}
