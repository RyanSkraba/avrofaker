package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Random, Try}

/** Given a schema, the AvroFaker creates data that matches it. */
sealed trait AvroFaker[T] {
  def generate(): T;
}

object AvroFaker {

  val PropMin: String = "min"
  val PropMax: String = "max"
  val PropStart: String = "start"
  val PropEnd: String = "end"
  val PropStep: String = "step"

  def apply(schema: Schema, rnd: Random = new Random()): AvroFaker[?] = {
    schema.getType match {
      case Schema.Type.RECORD  => RecordGenerator(schema, rnd)
      case Schema.Type.ENUM    => EnumGenerator(schema, rnd)
      case Schema.Type.ARRAY   => ArrayGenerator(schema, rnd)
      case Schema.Type.MAP     => MapGenerator(schema, rnd)
      case Schema.Type.UNION   => UnionGenerator(schema, rnd)
      case Schema.Type.FIXED   => FixedGenerator(schema, rnd)
      case Schema.Type.STRING  => StringGenerator(schema, rnd)
      case Schema.Type.BYTES   => BytesGenerator(schema, rnd)
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
case class RecordGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[GenericRecord] {
  private val internalGen: Map[Field, AvroFaker[?]] =
    schema.getFields.asScala.map((f: Field) => f -> AvroFaker(f.schema(), rnd)).toMap
  def generate(): GenericRecord = {
    val rb = new GenericRecordBuilder(schema)
    internalGen.map { case (f, fgen) => rb.set(f, fgen.generate()) }
    rb.build()
  }
}

/** An ENUM schema generates its Enum symbols (as strings) with equal probability.
  *
  * @param schema
  *   a schema of type RECORD
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class EnumGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[String] {
  private val symbols = schema.getEnumSymbols.asScala.toSeq
  def generate(): String = symbols(rnd.nextInt(symbols.size))
}

/** An ARRAY schema generates 2 to 5 elements of its element type
  *
  * @param schema
  *   a schema of type ARRAY
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class ArrayGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Any]] {
  private val internalGen: AvroFaker[?] = AvroFaker(schema.getElementType, rnd)
  def generate(): Array[Any] = Array.fill(2 + rnd.nextInt(3))(internalGen.generate())
}

/** A MAP schema generates 2 to 5 elements of its value type and a 10 character key
  *
  * @param schema
  *   a schema of type MAP
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class MapGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Map[String, Any]] {
  private val internalKGen: StringGenerator = StringGenerator(schema, rnd)
  private val internalVGen: AvroFaker[?] = AvroFaker(schema.getValueType, rnd)
  def generate(): Map[String, Any] =
    Array.fill(2 + rnd.nextInt(3))(internalKGen.generate() -> internalVGen.generate()).toMap
}

/** A UNION schema generates any of its possible schemas with equal probability.
  *
  * @param schema
  *   a schema of type UNION
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class UnionGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Any] {
  private val internalGen: Seq[AvroFaker[?]] = schema.getTypes.asScala.map(AvroFaker(_, rnd)).toSeq
  def generate(): Any = internalGen(rnd.nextInt(internalGen.size)).generate()
}

/** A FIXED schema generates a byte array of the expected size.
  *
  * @param schema
  *   a schema of type FIXED
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FixedGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Byte]] {
  def generate(): Array[Byte] = rnd.nextBytes(schema.getFixedSize)
}

/** A STRING schema generates a 10 character string.
  *
  * @param schema
  *   a schema of type STRING
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class StringGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[String] {
  def generate(): String = rnd.alphanumeric.take(10).mkString
}

/** A BYTES schema generates a byte array between 5 and 10 bytes.
  *
  * @param schema
  *   a schema of type BYTES
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BytesGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Byte]] {
  def generate(): Array[Byte] = rnd.nextBytes(5 + rnd.nextInt(5))
}

/** An INT schema generates an increasing sequence starting from zero, with the same strategies as [[LongGenerator]] but
  * constrained to Int values.
  *
  * @param schema
  *   a schema of type INT
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class IntGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Int] {
  private val internalGen = LongGenerator(schema, rnd = rnd)
  def generate(): Int = internalGen.generate().toInt
}

/** Generates LONG values with a specific strategy given by the schema properties.
  *
  *   - If [[AvroFaker.PropStart]], [[AvroFaker.PropEnd]] or [[AvroFaker.PropStep]] are specified, use a sequence
  *   - Otherwise generate a random number between [[AvroFaker.PropMin]] and [[AvroFaker.PropMax]]
  *
  * @param schema
  *   a schema of type INT or LONG. Although the generated data are Long values, they will be constrained to Integer
  *   minimums and maximums if this is an INT schema.
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class LongGenerator(
    schema: Schema,
    rnd: Random = new Random()
) extends AvroFaker[Long] {
  private val (minimum: Long, maximum: Long) =
    if (schema.getType == Schema.Type.INT) (Int.MinValue.toLong, Int.MaxValue.toLong)
    else (Long.MinValue, Long.MaxValue)
  private val internalGen =
    if (schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStep))
      LongSequenceGenerator(
        start = Option(schema.getProp(PropStart)).map(_.toLong).getOrElse(0L),
        end = Option(schema.getProp(PropEnd)).map(_.toLong).getOrElse(maximum),
        step = Option(schema.getProp(PropStep)).map(_.toLong).getOrElse(1L)
      )
    else
      LongRandomGenerator(
        min = Option(schema.getProp(PropMin)).map(_.toLong).getOrElse(minimum),
        max = Option(schema.getProp(PropMax)).map(_.toLong).getOrElse(maximum),
        rnd
      )
  def generate(): Long = internalGen.generate()
}

/** A generator that generates whole numbers from `start` (inclusive) to `end` (exclusive), counting by `step`. When the
  * end of the sequence is reached, it repeats.
  * @param start
  * @param end
  * @param step
  */
case class LongSequenceGenerator(start: Long = 0, end: Long = Long.MaxValue, step: Long = 1) extends AvroFaker[Long] {
  private var current: Long = start
  def generate(): Long = {
    val next = current;
    current = Try(math.addExact(current, step)).getOrElse(start)
    if (current >= end) current = start
    next
  }
}

/** A generator that generates whole numbers from `start` (inclusive) to `end` (exclusive), counting by `step`. When the
  * end of the sequence is reached, it repeats.
  * @param start
  * @param end
  * @param step
  */
case class LongRandomGenerator(min: Long = 0, max: Long = Long.MaxValue, rnd: Random = new Random())
    extends AvroFaker[Long] {
  def generate(): Long = rnd.between(min, max)
}

/** A FLOAT schema generates a random floating point number
  *
  * @param schema
  *   a schema of type DOUBLE
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FloatGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Float] {
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
case class DoubleGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Double] {
  def generate(): Double = rnd.nextDouble()
}

/** A BOOLEAN schema generates random true/false.
  *
  * @param schema
  *   a schema of type BOOLEAN
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BooleanGenerator(schema: Schema, rnd: Random) extends AvroFaker[Boolean] {
  def generate(): Boolean = rnd.nextBoolean()
}

/** A NULL schema generates only null.
  *
  * @param schema
  *   a schema of type NULL
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class NullGenerator(schema: Schema) extends AvroFaker[Void] {
  def generate(): Void = null
}
