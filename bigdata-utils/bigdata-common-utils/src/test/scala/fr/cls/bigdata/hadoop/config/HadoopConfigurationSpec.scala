package fr.cls.bigdata.hadoop.config

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.ftp.FTPFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.scalatest.{FunSpec, Matchers}

class HadoopConfigurationSpec extends FunSpec with Matchers {
  describe("companion.apply") {
    it("should load the reference file") {
      val emptyConfig = ConfigFactory.load()

      val hadoopConfig = HadoopConfiguration(emptyConfig)

      hadoopConfig.getClass("fs.hdfs.impl", null) shouldBe classOf[DistributedFileSystem]
      hadoopConfig.getClass("fs.file.impl", null) shouldBe classOf[LocalFileSystem]
    }

    it("should override reference") {
      val overrideConfig = ConfigFactory.parseString(
        """
          |fr.cls.bigdata.hadoop {
          |  configuration {
          |    fs.hdfs.impl = "org.apache.hadoop.hdfs.web.WebHdfsFileSystem"
          |    fs.file.impl = "org.apache.hadoop.fs.ftp.FTPFileSystem"
          |    test = ["value1", "value2"]
          |  }
          |}
        """.stripMargin)

      val hadoopConfig = HadoopConfiguration(overrideConfig)

      hadoopConfig.getClass("fs.hdfs.impl", null) shouldBe classOf[WebHdfsFileSystem]
      hadoopConfig.getClass("fs.file.impl", null) shouldBe classOf[FTPFileSystem]
      hadoopConfig.get("test") shouldBe "value1,value2"
    }
  }

}
