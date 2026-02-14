package com.skraba.avrofaker

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Random

/** Given a schema, the AvroGenerator creates data that matches it. */
sealed trait AvroFaker {
  def generate(): Any;
}

object AvroFaker {
  def apply(schema: Schema, rnd: Random = new Random()): AvroFaker = {
    schema.getType match {
      case Schema.Type.RECORD  => RecordGenerator(schema, rnd)
      case Schema.Type.ENUM    => ???
      case Schema.Type.ARRAY   => ???
      case Schema.Type.MAP     => ???
      case Schema.Type.UNION   => ???
      case Schema.Type.FIXED   => ???
      case Schema.Type.STRING  => StringGenerator(schema, rnd)
      case Schema.Type.BYTES   => ???
      case Schema.Type.INT     => IntGenerator(schema, rnd)
      case Schema.Type.LONG    => ???
      case Schema.Type.FLOAT   => ???
      case Schema.Type.DOUBLE  => ???
      case Schema.Type.BOOLEAN => ???
      case Schema.Type.NULL    => NullGenerator(schema)
    }
  }
}

/** A RECORD schema generates field data according to the schema of its fields.
  *
  * @param schema
  *   a schema of type RECORD
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class RecordGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  assert(schema.getType == Schema.Type.RECORD, s"${Schema.Type.RECORD} required, ${schema.getType} found")
  val fieldGenerators: Map[Field, AvroFaker] =
    schema.getFields.asScala.map((f: Field) => f -> AvroFaker(f.schema(), rnd)).toMap
  def generate(): GenericRecord = {
    val rb = new GenericRecordBuilder(schema)
    fieldGenerators.map { case (f, fgen) => rb.set(f, fgen.generate()) }
    rb.build()
  }
}

/** A STRING schema generates a 10 character string.
  *
  * @param schema
  *   a schema of type STRING
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class StringGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  assert(schema.getType == Schema.Type.STRING, s"${Schema.Type.STRING} required, ${schema.getType} found")
  def generate(): String = rnd.alphanumeric.take(10).mkString
}

/** An INT schema generates an increasing sequence starting from zero.
  *
  * @param schema
  *   a schema of type INT
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class IntGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  assert(schema.getType == Schema.Type.INT, s"${Schema.Type.INT} required, ${schema.getType} found")
  var seq: Int = -1
  def generate(): Int = { seq += 1; seq }
}

/** A NULL schema generates only null.
  *
  * @param schema
  *   a schema of type NULL
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class NullGenerator(schema: Schema) extends AvroFaker {
  assert(schema.getType == Schema.Type.NULL, s"${Schema.Type.NULL} required, ${schema.getType} found")
  def generate(): Any = null
}
