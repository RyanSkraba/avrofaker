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
      case Schema.Type.LONG    => LongGenerator(schema, rnd)
      case Schema.Type.FLOAT   => FloatGenerator(schema, rnd)
      case Schema.Type.DOUBLE  => DoubleGenerator(schema, rnd)
      case Schema.Type.BOOLEAN => BooleanGenerator(schema, rnd)
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
  private val fieldGenerators: Map[Field, AvroFaker] =
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
  private val internalGen = LongGenerator(schema, rnd)
  def generate(): Int = internalGen.generate().toInt
}

/** A LONG schema generates an increasing sequence starting from zero.
  *
  * @param schema
  *   a schema of type LONG
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class LongGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  private var seq: Long = -1
  def generate(): Long = { seq += 1; seq }
}

/** A FLOAT schema generates a random floating point number
  *
  * @param schema
  *   a schema of type DOUBLE
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FloatGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  private val internalGen = DoubleGenerator(schema, rnd)
  def generate(): Float = internalGen.generate().toFloat
}

/** A DOUBLE schema generates a random floating point number
  *
  * @param schema
  *   a schema of type DOUBLE
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class DoubleGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker {
  def generate(): Double = rnd.nextDouble()
}

/** A BOOLEAN schema generates random true/false.
  *
  * @param schema
  *   a schema of type BOOLEAN
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BooleanGenerator(schema: Schema, rnd: Random) extends AvroFaker {
  def generate(): Any = rnd.nextBoolean()
}

/** A NULL schema generates only null.
  *
  * @param schema
  *   a schema of type NULL
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class NullGenerator(schema: Schema) extends AvroFaker {
  def generate(): Any = null
}
