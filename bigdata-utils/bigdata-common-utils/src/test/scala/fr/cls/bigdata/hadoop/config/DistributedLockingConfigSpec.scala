package fr.cls.bigdata.hadoop.config

import java.io.File

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}

class DistributedLockingConfigSpec extends FunSpec with Matchers {
  describe("DistributedLockingConfig") {
    it("should read properly a valid configuration") {
      val config = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.hadoop.distributed-lock {
          |   lock-folder = "/tmp-folder"
          |   stripe-count = 15
          |   timeout = 5 minutes
          |}
        """.stripMargin)

      val result = DistributedLockingConfig(config)

      result.lockFolder shouldBe new File("/tmp-folder")
      result.stripeCount shouldBe 15
      result.timeoutMs shouldBe 300000L
    }

    it("should read default values without errors") {
      val config = ConfigFactory.load()

      val result = DistributedLockingConfig(config)

      result.lockFolder shouldBe new File("/tmp")
      result.stripeCount shouldBe 100
      result.timeoutMs shouldBe 900000L
    }

    it("should throw a ConfigException on invalid configuration") {
      val config = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.hadoop.distributed-lock {
          |   lock-folder = []
          |}
        """.stripMargin)

      a[ConfigException] should be thrownBy DistributedLockingConfig(config)
    }
  }
}
