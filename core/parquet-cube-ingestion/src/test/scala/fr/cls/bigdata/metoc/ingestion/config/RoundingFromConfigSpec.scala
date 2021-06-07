package fr.cls.bigdata.metoc.ingestion.config

import com.typesafe.config.{ConfigException, ConfigFactory}
import fr.cls.bigdata.georef.utils.Rounding
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers}

class RoundingFromConfigSpec extends FunSpec with TableDrivenPropertyChecks with Matchers {
  describe("apply") {
    it("should throw ConfigException coordinates-precision is invalid") {
      val config = ConfigFactory.parseString(
        """
          |coordinates-precision = TTT
          |rounding-mode = RoundUp
        """.stripMargin)

      a[ConfigException] should be thrownBy RoundingFromConfig(config)
    }

    it("should throw ConfigException rounding-mode is invalid") {
      val config = ConfigFactory.parseString(
        """
          |coordinates-precision = 3
          |rounding-mode = TT
        """.stripMargin)

      a[ConfigException] should be thrownBy RoundingFromConfig(config)
    }

    it("should parse config when it is valid") {
      val config = ConfigFactory.parseString(
        """
          |coordinates-precision = 3
          |rounding-mode = RoundUp
        """.stripMargin)

      val rounding = RoundingFromConfig(config)

      rounding.precision shouldBe 3
      rounding.roundingMode shouldBe Rounding.RoundUp
    }
  }
}
