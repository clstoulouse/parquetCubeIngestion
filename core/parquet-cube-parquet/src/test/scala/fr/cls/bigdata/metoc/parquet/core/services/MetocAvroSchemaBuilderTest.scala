package fr.cls.bigdata.metoc.parquet.core.services

import fr.cls.bigdata.georef.model.{Dimensions, VariableRef}
import fr.cls.bigdata.metoc.model.GeoReference
import fr.cls.bigdata.metoc.parquet.utils.MetocParquetTestUtils
import org.apache.avro.Schema
import org.scalatest.{FunSpec, Matchers}

class MetocAvroSchemaBuilderTest extends FunSpec with Matchers with MetocParquetTestUtils {

  private val schemaBuilder = MetocAvroSchemaBuilder

  describe("MetocParquetAvroSchemaBuilder") {

    describe("for 3d-only datasets") {
      testTimeColumn(variable3Ds, GeoReference.Only3D)
      testLongitudeColumn(variable3Ds, GeoReference.Only3D)
      testLatitudeColumn(variable3Ds, GeoReference.Only3D)
      testVariablesColumns(variable3Ds, GeoReference.Only3D, firstVarColumnPosition = 3)

      it(s"should NOT have a '${Dimensions.depth.name}' column") {
        // perform
        val avroSchema = schemaBuilder.buildSchema(variable3Ds, GeoReference.Only3D)

        // validate
        val field = avroSchema.getField(Dimensions.depth.name)

        field should be(null) // field should NOY exist
      }
    }

    describe("for 4d-only datasets") {
      testTimeColumn(variable4Ds, GeoReference.Only4D)

      testLongitudeColumn(variable4Ds, GeoReference.Only4D)

      testLatitudeColumn(variable4Ds, GeoReference.Only4D)

      testVariablesColumns(variable4Ds, GeoReference.Only4D, firstVarColumnPosition = 4)

      it(s"should have a '${Dimensions.depth.name}' column of type double") {
        // perform
        val avroSchema = schemaBuilder.buildSchema(variable4Ds, GeoReference.Only4D)

        // validate
        val field = avroSchema.getField(Dimensions.depth.name)

        field should not be null // field should exist
        field.pos() should be(3) // field should have position 0
        field.schema().getType should be(Schema.Type.DOUBLE) // field should have type double
      }
    }

    describe("for mixed 3d/4d datasets") {
      testTimeColumn(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D)
      testLongitudeColumn(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D)
      testLatitudeColumn(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D)
      testVariablesColumns(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D, firstVarColumnPosition = 4)

      it(s"should have a '${Dimensions.depth.name}' column of type 'optional double'") {
        import scala.collection.JavaConverters._

        // perform
        val avroSchema = schemaBuilder.buildSchema(mixed3DAnd4DVariables, GeoReference.Mixed3DAnd4D)

        // validate
        val field = avroSchema.getField(Dimensions.depth.name)

        field should not be null // field should exist
        field.pos() should be(3) // field should have position 0
        field.schema().getType should be(Schema.Type.UNION) // field should have type UNION
        field.schema().getTypes.asScala.map(_.getType) should contain theSameElementsAs Set(Schema.Type.DOUBLE, Schema.Type.NULL) // should allow double and nulls
      }
    }

  }

  private def testTimeColumn(variables: Set[VariableRef], reference: GeoReference): Unit =
    it(s"should have a '${Dimensions.time.name}' column of type long") {
      // perform
      val avroSchema = schemaBuilder.buildSchema(variables, reference)

      // validate
      val field = avroSchema.getField(Dimensions.time.name)

      field should not be null // field should exist
      field.pos() should be(0) // field should have position 0
      field.schema().getType should be(Schema.Type.LONG) // field should have type long
    }

  private def testLongitudeColumn(variables: Set[VariableRef], reference: GeoReference): Unit =
    it(s"should have a '${Dimensions.longitude.name}' column of type double") {
      // perform
      val avroSchema = schemaBuilder.buildSchema(variables, reference)

      // validate
      val field = avroSchema.getField(Dimensions.longitude.name)

      field should not be null // field should exist
      field.pos() should be(1) // field should have position 1
      field.schema().getType should be(Schema.Type.DOUBLE) // field should have type double
    }

  private def testLatitudeColumn(variables: Set[VariableRef], reference: GeoReference): Unit =
    it(s"should have a '${Dimensions.latitude.name}' column of type double") {
      // perform
      val avroSchema = schemaBuilder.buildSchema(variables, reference)

      // validate
      val field = avroSchema.getField(Dimensions.latitude.name)

      field should not be null // field should exist
      field.pos() should be(2) // field should have position 2
      field.schema().getType should be(Schema.Type.DOUBLE) // field should have type double
    }

  private def testVariablesColumns(variables: Set[VariableRef], reference: GeoReference, firstVarColumnPosition: Int): Unit =
    it(s"should have a column of type 'optional double' for each variable named after their standard name") {
      import scala.collection.JavaConverters._

      // perform
      val avroSchema = schemaBuilder.buildSchema(variables, reference)

      // validate
      for ((variable, i) <- variables.toSeq.sortBy(_.name).zipWithIndex) {
        val field = avroSchema.getField(variable.name)

        field should not be null // field should exist
        field.pos() should be(i + firstVarColumnPosition) // field should have position 2
        field.schema().getType should be(Schema.Type.UNION) // field should have type UNION
        field.schema().getTypes.asScala.map(_.getType) should contain theSameElementsAs Set(Schema.Type.DOUBLE, Schema.Type.NULL) // should allow double and nulls
      }
    }
}
