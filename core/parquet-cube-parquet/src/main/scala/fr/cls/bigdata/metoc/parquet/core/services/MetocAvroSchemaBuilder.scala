package fr.cls.bigdata.metoc.parquet.core.services


import fr.cls.bigdata.georef.model.{Dimensions, VariableRef}
import fr.cls.bigdata.metoc.model.GeoReference
import org.apache.avro.{Schema, SchemaBuilder}

trait MetocAvroSchemaBuilder {

  /**
    * Builds a new avro schema corresponding to a metoc dataset.
    *
    * @param variables Set of metoc variables.
    * @param reference The reference of the variables, 3D, 4D or mixed
    * @return The created avro schema.
    */
  def buildSchema(variables: Set[VariableRef], reference: GeoReference): Schema
}

private[parquet] object MetocAvroSchemaBuilder extends MetocAvroSchemaBuilder {

  private final val schemaNamespace = "fr.cls.parquet"
  private final val schemaRecordName = "netcdfAvro"

  private val variablesOrdering: Ordering[VariableRef] = Ordering.by(_.name)

  override def buildSchema(variables: Set[VariableRef], reference: GeoReference): Schema = {
    val schemaBuilder = SchemaBuilder.record(schemaRecordName)
      .namespace(schemaNamespace)
      .fields

    addTimeColumn(schemaBuilder)
    addLongitudeColumn(schemaBuilder)
    addLatitudeColumn(schemaBuilder)

    reference match {
      case GeoReference.Only4D => addDepthColumn(schemaBuilder, isRequired = true)
      case GeoReference.Mixed3DAnd4D => addDepthColumn(schemaBuilder, isRequired = false)
      case GeoReference.Only3D => ()
    }

    for (variable <- variables.toSeq.sortBy(identity)(variablesOrdering)) {
      addVariableColumn(schemaBuilder, variable)
    }

    schemaBuilder.endRecord
  }

  private def addTimeColumn(schemaBuilder: SchemaBuilder.FieldAssembler[Schema]): Unit = {
    schemaBuilder.name(Dimensions.time.name).`type`.longType.noDefault
  }

  private def addLongitudeColumn(schemaBuilder: SchemaBuilder.FieldAssembler[Schema]): Unit = {
    schemaBuilder.name(Dimensions.longitude.name).`type`.doubleType.noDefault
  }

  private def addLatitudeColumn(schemaBuilder: SchemaBuilder.FieldAssembler[Schema]): Unit = {
    schemaBuilder.name(Dimensions.latitude.name).`type`.doubleType.noDefault
  }

  private def addDepthColumn(schemaBuilder: SchemaBuilder.FieldAssembler[Schema], isRequired: Boolean): Unit = {
    val rawTypeBuilder = schemaBuilder.name(Dimensions.depth.name).`type`
    if (isRequired) rawTypeBuilder.doubleType.noDefault else rawTypeBuilder.optional.doubleType
  }

  private def addVariableColumn(schemaBuilder: SchemaBuilder.FieldAssembler[Schema], variable: VariableRef): Unit = {
    schemaBuilder.name(variable.name).`type`.optional.doubleType
  }
}
