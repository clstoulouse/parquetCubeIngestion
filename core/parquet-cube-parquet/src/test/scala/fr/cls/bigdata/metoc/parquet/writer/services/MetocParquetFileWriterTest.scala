package fr.cls.bigdata.metoc.parquet.writer.services


import fr.cls.bigdata.georef.model.Dimensions
import fr.cls.bigdata.hadoop.HadoopTestUtils
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.metoc.exceptions.MetocWriterException
import fr.cls.bigdata.metoc.model.Coordinates
import fr.cls.bigdata.metoc.parquet.utils.MetocParquetTestUtils
import fr.cls.bigdata.metoc.parquet.writer.objs.MetocParquetFileWriterConfiguration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalatest.{FunSpec, Matchers}

class MetocParquetFileWriterTest extends FunSpec with Matchers with MetocParquetTestUtils with HadoopTestUtils {
  private val config = MetocParquetFileWriterConfiguration(CompressionCodecName.SNAPPY,
    HadoopConfiguration)

  describe("MetocParquetFileWriter") {
    describe(".open()") {
      it("should create a an empty file if it does not exist") {
        // prepare
        val parquetFile = generateTempFile()

        // perform
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        writer.close()

        // validate
        val records = readParquetFileRecords(parquetFile)

        records should have size 0
      }

      it("should overwrite the file without errors if it already exists") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test

        // perform
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        writer.close()

        // validate
        val records = readParquetFileRecords(parquetFile)

        records should have size 0
      }

      it(s"should throw a ${classOf[MetocWriterException].getSimpleName} in case of IO error") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test
        simulateNoPermissionsOn(parquetFile)

        // perform & validate
        an[MetocWriterException] should be thrownBy MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
      }
    }

    describe(".writeDataPoint") {
      it("should write a new row each time it is called") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test

        // perform
        val n = 5
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        try {
          for (i <- 1 to n) {
            writer.writeDataPoint(Coordinates(time = i, latitude = 2, longitude = 3, depth = None), Seq())
          }
        } finally {
          writer.close()
        }
        writer.close()

        // validate
        val records = readParquetFileRecords(parquetFile)

        records should have size n
      }

      it(s"should write the time in the column ${Dimensions.time.name}") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test

        // perform
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        try {
          writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
        } finally {
          writer.close()
        }

        // validate
        val record = readParquetFileRecords(parquetFile).head

        record.get(Dimensions.time.name) should be(Some(1))
      }

      it(s"should write the latitude in the column ${Dimensions.latitude.name}") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test

        // perform
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        try {
          writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
        } finally {
          writer.close()
        }

        // validate
        val record = readParquetFileRecords(parquetFile).head

        record.get(Dimensions.latitude.name) should be(Some(2))
      }

      it(s"should write the longitude in the column ${Dimensions.longitude.name}") {
        // prepare
        val parquetFile = generateTempFile()
        createEmptyFile(parquetFile) // create an empty file before playing the test

        // perform
        val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
        try {
          writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
        } finally {
          writer.close()
        }

        // validate
        val record = readParquetFileRecords(parquetFile).head

        record.get(Dimensions.longitude.name) should be(Some(3))
      }

      describe("if the depth is required") {
        it(s"should write the depth in the column ${Dimensions.depth.name}") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema4dOnly, config)
          try {
            writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = Some(4)), Seq())
          } finally {
            writer.close()
          }

          // validate
          val record = readParquetFileRecords(parquetFile).head

          record.get(Dimensions.depth.name) should be(Some(4))
        }

        it(s"should throw a ${classOf[MetocWriterException].getSimpleName} if the depth is missing from the ccordinates") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform & validate
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema4dOnly, config)
          try {
            an[MetocWriterException] should be thrownBy writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
          } finally {
            writer.close()
          }
        }
      }

      describe("if the depth is optional") {
        it(s"should write the depth in the column ${Dimensions.depth.name} when specified") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchemaMixed3dAnd4d, config)
          try {
            writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = Some(4)), Seq())
          } finally {
            writer.close()
          }

          // validate
          val record = readParquetFileRecords(parquetFile).head

          record.get(Dimensions.depth.name) should be(Some(4))
        }

        it(s"should write NULL in the column ${Dimensions.depth.name} when the depth is missing") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchemaMixed3dAnd4d, config)
          try {
            writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
          } finally {
            writer.close()
          }

          // validate
          val record = readParquetFileRecords(parquetFile).head

          record.hasField(Dimensions.depth.name) should be(true)
          record.get(Dimensions.depth.name) should be(None)
        }
      }

      describe("if the depth is un-needed") {

        it(s"should throw a ${classOf[MetocWriterException].getSimpleName} if the coordinates has depth") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform & validate
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
          try {
            an[MetocWriterException] should be thrownBy writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = Some(4)), Seq())
          } finally {
            writer.close()
          }
        }
      }
    }

    it(s"should write the variable values in its column if a value is specified") {
      // prepare
      val parquetFile = generateTempFile()
      createEmptyFile(parquetFile) // create an empty file before playing the test

      // perform
      val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
      try {
        writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq(Var3d_1 -> Some(5)))
      } finally {
        writer.close()
      }

      // validate
      val record = readParquetFileRecords(parquetFile).head

      record.get(Var3d_1.name) should be(Some(5))
    }

    it(s"should write NULL in the variable column if a fill value is specified") {
      // prepare
      val parquetFile = generateTempFile()
      createEmptyFile(parquetFile) // create an empty file before playing the test

      // perform
      val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
      try {
        writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq(Var3d_1 -> None))
      } finally {
        writer.close()
      }

      // validate
      val record = readParquetFileRecords(parquetFile).head

      record.hasField(Var3d_1.name) should be(true)
      record.get(Var3d_1.name) should be(None)
    }

    it(s"should write NULL in the variable column if no value is specified") {
      // prepare
      val parquetFile = generateTempFile()
      createEmptyFile(parquetFile) // create an empty file before playing the test

      // perform
      val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)
      try {
        writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq(Var3d_1 -> None))
      } finally {
        writer.close()
      }

      // validate
      val record = readParquetFileRecords(parquetFile).head

      record.hasField(Var3d_2.name) should be(true)
      record.get(Var3d_2.name) should be(None)
    }

    it(s"should throw ${classOf[MetocWriterException].getSimpleName} if a variable is unknown in the schema") {
      // prepare
      val parquetFile = generateTempFile()
      createEmptyFile(parquetFile) // create an empty file before playing the test

      // perform & validate
      val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config)

      try {
        an[MetocWriterException] should be thrownBy writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq(Var4d_1 -> Some(5)))
      } finally {
        writer.close()
      }
    }

    describe("compression codec") {
      for (compressionCodecName <- Set(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY, CompressionCodecName.GZIP)) {
        it(s"'$compressionCodecName' should be supported") {
          // prepare
          val parquetFile = generateTempFile()
          createEmptyFile(parquetFile) // create an empty file before playing the test

          // perform
          val writer = MetocParquetFileWriter.open(parquetFile, AvroSchema3dOnly, config.copy(compressionCodec = compressionCodecName))
          try {
            writer.writeDataPoint(Coordinates(time = 1, latitude = 2, longitude = 3, depth = None), Seq())
          } finally {
            writer.close()
          }

          // validate
          getCompressionCodec(parquetFile) should be(compressionCodecName)
        }
      }

    }
  }

  private def getCompressionCodec(parquetFile: PathWithFileSystem): CompressionCodecName = {
    val parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(parquetFile.path, HadoopConfiguration))
    try {
      parquetFileReader.getRowGroups.get(0).getColumns.get(0).getCodec
    } finally {
      parquetFileReader.close()
    }
  }
}
