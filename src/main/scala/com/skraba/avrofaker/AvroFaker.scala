package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Random, Try}

/** AvroFaker creates generic Avro data from an annotated Avro schema.
  *
  * As a basic example, to generate random integers between 100 and 200:
  *
  * {{{
  * val schema = """{"type": "int", "min": 100, "max": 200}"""
  * val faker = AvroFaker(new org.apache.avro.Schema.Parser().parse(schema))
  *
  * // prints 194 134 191 104 181 117 148 133 122 174 130
  * for (_ <- 0 to 10) print(s"${faker()} ")
  * }}}
  *
  * Every schema and subschema can be annotated, so you can create awesome, customizable fake data for testing!
  */
object AvroFaker {

  val PropMin: String = "min"
  val PropMax: String = "max"

  val PropStart: String = "start"
  val PropEnd: String = "end"
  val PropStep: String = "step"

  val PropMean: String = "mean"
  val PropStdDev: String = "stddev"

  val PropLength: String = "length"
  val PropFaker: String = "faker"

  def apply(schema: Schema, rnd: Random = new Random()): AvroFaker[_] = {
    schema.getType match {
      case Schema.Type.RECORD  => RecordGenerator(schema, rnd = rnd)
      case Schema.Type.ENUM    => EnumGenerator(schema, rnd = rnd)
      case Schema.Type.ARRAY   => ArrayGenerator(schema, rnd = rnd)
      case Schema.Type.MAP     => MapGenerator(schema, rnd = rnd)
      case Schema.Type.UNION   => UnionGenerator(schema, rnd = rnd)
      case Schema.Type.FIXED   => FixedGenerator(schema, rnd = rnd)
      case Schema.Type.STRING  => StringGenerator(schema, rnd = rnd)
      case Schema.Type.BYTES   => BytesGenerator(schema, rnd = rnd)
      case Schema.Type.INT     => IntGenerator(schema, rnd = rnd)
      case Schema.Type.LONG    => LongGenerator(schema, rnd = rnd)
      case Schema.Type.FLOAT   => FloatGenerator(schema, rnd = rnd)
      case Schema.Type.DOUBLE  => DoubleGenerator(schema, rnd = rnd)
      case Schema.Type.BOOLEAN => BooleanGenerator(schema, rnd = rnd)
      case Schema.Type.NULL    => NullGenerator(schema)
    }
  }

  def getLong(value: Any): () => Long = () => value.toString.toLong

  /** Get a Long value from the schema.
    *
    * @param schema
    *   The schema that may have a set of properties.
    * @param key
    *   The name of the property to fetch from the schema.
    * @param dflts
    *   If the property doesn't exist in the schema, potentially a default to use.
    * @param fallback
    *   If there is no property and no default, the "final" fallback to use.
    * @return
    *   The extracted value of the property.
    */
  def getLong(
      schema: Schema,
      key: String,
      fallback: Long,
      dflts: PartialFunction[String, Any] = PartialFunction.empty
  ): () => Long =
    Option(schema.getObjectProp(key)).orElse(dflts.lift(key)).map(getLong).getOrElse(getLong(fallback))

  def getDouble(value: Any): () => Double = () => value.toString.toDouble

  /** Get a Double value from the schema.
    *
    * @param schema
    *   The schema that may have a set of properties.
    * @param key
    *   The name of the property to fetch from the schema.
    * @param dflts
    *   If the property doesn't exist in the schema, potentially a default to use.
    * @param fallback
    *   If there is no property and no default, the "final" fallback to use.
    * @return
    *   The extracted value of the property.
    */
  def getDouble(
      schema: Schema,
      key: String,
      fallback: Double,
      dflts: PartialFunction[String, Any] = PartialFunction.empty
  ): () => Double =
    Option(schema.getObjectProp(key)).orElse(dflts.lift(key)).map(getDouble).getOrElse(getDouble(fallback))
}

/** Each AvroFaker creates a type of datum, depending on the schema. */
sealed trait AvroFaker[T] extends (() => T) {
  def apply(): T
}

/** A RECORD schema generates field data according to the schema of its fields.
  *
  * @param schema
  *   a schema of type RECORD
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class RecordGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[GenericRecord] {
  private val fn: Map[Field, AvroFaker[?]] =
    schema.getFields.asScala.map((f: Field) => f -> AvroFaker(f.schema(), rnd)).toMap
  def apply(): GenericRecord = {
    val rb = new GenericRecordBuilder(schema)
    fn.map { case (f, fgen) => rb.set(f, fgen.apply()) }
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
  private val indexFn = IntGenerator(
    schema,
    rnd = rnd,
    dflts = Map(PropMin -> 0, PropMax -> symbols.size, PropStart -> 0, PropEnd -> symbols.size)
  )
  def apply(): String = symbols(indexFn())
}

/** An ARRAY schema generates 2 to 5 elements of its element type
  *
  * @param schema
  *   a schema of type ARRAY
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class ArrayGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Any]] {
  private val fn: AvroFaker[?] = AvroFaker(schema.getElementType, rnd)
  def apply(): Array[Any] = Array.fill(2 + rnd.nextInt(3))(fn.apply())
}

/** A MAP schema generates 2 to 5 elements of its value type and a 10 character key
  *
  * @param schema
  *   a schema of type MAP
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class MapGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Map[String, Any]] {
  private val kFn: StringGenerator = StringGenerator(schema, rnd)
  private val vFn: AvroFaker[?] = AvroFaker(schema.getValueType, rnd)
  def apply(): Map[String, Any] =
    Array.fill(2 + rnd.nextInt(3))(kFn.apply() -> vFn.apply()).toMap
}

/** A UNION schema generates any of its possible schemas with equal probability.
  *
  * @param schema
  *   a schema of type UNION
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class UnionGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Any] {
  private val fns: Seq[AvroFaker[?]] = schema.getTypes.asScala.map(AvroFaker(_, rnd)).toSeq
  def apply(): Any = fns(rnd.nextInt(fns.size)).apply()
}

/** A FIXED schema generates a byte array of the expected size.
  *
  * @param schema
  *   a schema of type FIXED
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FixedGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Byte]] {
  def apply(): Array[Byte] = rnd.nextBytes(schema.getFixedSize)
}

/** A STRING schema generator
  *
  * @param schema
  *   a schema of type STRING
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class StringGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[String] {
  private val fn =
    if (schema.propsContainsKey(PropFaker))
      StringFakerGenerator(schema.getProp(PropFaker))
    else
      StringRandomGenerator(length = getLong(schema, PropLength, 10, PartialFunction.empty))

  def apply(): String = fn.apply()

  /** Generates a random string of the expected length
    *
    * @param length
    *   the size of the string to generate
    */
  private[this] case class StringRandomGenerator(length: () => Long) extends AvroFaker[String] {
    def apply(): String = rnd.alphanumeric.take(length().toInt).mkString
  }

  /** Uses Faker to generate a string
    *
    * @param expression
    *   the Faker expression to generate
    */
  private[this] case class StringFakerGenerator(expression: String) extends AvroFaker[String] {
    val faker: Faker = new Faker(new java.util.Random(rnd.nextLong()))
    def apply(): String = faker.expression(expression)
  }
}

/** A BYTES schema generates a byte array between 5 and 10 bytes.
  *
  * @param schema
  *   a schema of type BYTES
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BytesGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Array[Byte]] {
  def apply(): Array[Byte] = rnd.nextBytes(5 + rnd.nextInt(5))
}

/** An INT schema generates an increasing sequence starting from zero, with the same strategies as [[LongGenerator]] but
  * constrained to Int values.
  *
  * @param schema
  *   a schema of type INT
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class IntGenerator(
    schema: Schema,
    rnd: Random = new Random(),
    dflts: PartialFunction[String, Any] = PartialFunction.empty
) extends AvroFaker[Int] {
  private val fn =
    LongGenerator(
      schema,
      rnd = rnd,
      dflts = dflts.orElse(Map(PropEnd -> Int.MaxValue, PropMax -> Int.MaxValue, PropMin -> Int.MinValue))
    )
  def apply(): Int = fn.apply().toInt
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
    rnd: Random = new Random(),
    dflts: PartialFunction[String, Any] = PartialFunction.empty
) extends AvroFaker[Long] {
  private val fn =
    if (schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStep))
      LongSequenceGenerator(
        start = getLong(schema, PropStart, 0, dflts),
        end = getLong(schema, PropEnd, Long.MaxValue, dflts),
        step = getLong(schema, PropStep, 1, dflts)
      )
    else
      LongRandomGenerator(
        min = getLong(schema, PropMin, Long.MinValue, dflts),
        max = getLong(schema, PropMax, Long.MaxValue, dflts)
      )

  def apply(): Long = fn.apply()

  /** A generator that generates whole numbers from `start` (inclusive) to `end` (exclusive), counting by `step`. When
    * the end of the sequence is reached, it repeats.
    * @param start
    *   The first number in the sequence
    * @param end
    *   The last number in the sequence (exclusive)
    * @param step
    *   The step to count by
    */
  private[this] case class LongSequenceGenerator(start: () => Long, end: () => Long, step: () => Long)
      extends AvroFaker[Long] {
    private var current: Long = start()
    def apply(): Long = {
      lazy val lzyStart = start()
      lazy val lzyEnd = end()
      lazy val lzyStep = step()
      val next = current
      current = Try(math.addExact(current, lzyStep)).getOrElse(lzyStart)
      if (current >= lzyEnd) current = lzyStart + current - lzyEnd
      next
    }
  }

  /** A generator that generates whole numbers from `start` (inclusive) to `end` (exclusive), counting by `step`. When
    * the end of the sequence is reached, it repeats.
    * @param min
    *   The smallest number to be generated, or the lower limit.
    * @param max
    *   The upper limit (exclusive), or one more than the largest number to be generated.
    */
  private[this] case class LongRandomGenerator(min: () => Long, max: () => Long) extends AvroFaker[Long] {
    def apply(): Long = rnd.between(min(), max())
  }
}

/** A FLOAT schema generates a random floating point number
  *
  * @param schema
  *   a schema of type FLOAT
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FloatGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Float] {
  private val internalGen = DoubleGenerator(schema, rnd)
  def apply(): Float = internalGen.apply().toFloat
}

/** A DOUBLE schema generates a random floating point number
  *
  * @param schema
  *   a schema of type DOUBLE
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class DoubleGenerator(schema: Schema, rnd: Random = new Random()) extends AvroFaker[Double] {
  private val fn =
    if (schema.propsContainsKey(PropMean) || schema.propsContainsKey(PropStdDev))
      DoubleGaussGenerator(
        mean = getDouble(schema, PropMean, 0),
        stdDev = getDouble(schema, PropStdDev, 1)
      )
    else if (
      schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStart) || schema.propsContainsKey(PropStep)
    )
      DoubleSequenceGenerator(
        start = getDouble(schema, PropStart, 0),
        end = getDouble(schema, PropEnd, Double.PositiveInfinity),
        step = getDouble(schema, PropStep, 1)
      )
    else
      DoubleRandomGenerator(
        min = getDouble(schema, PropMin, 0),
        max = getDouble(schema, PropMax, 1)
      )

  def apply(): Double = fn.apply()

  /** Generates a random double
    *
    * @param min
    *   The smallest number to be generated, or the lower limit.
    * @param max
    *   The upper limit (exclusive)
    */
  private[this] case class DoubleRandomGenerator(min: () => Double, max: () => Double) extends AvroFaker[Double] {
    def apply(): Double = rnd.between(min(), max())
  }

  /** Generates a double along the gaussian distribution */
  private[this] case class DoubleGaussGenerator(mean: () => Double, stdDev: () => Double) extends AvroFaker[Double] {
    def apply(): Double = rnd.nextGaussian() * stdDev() + mean()
  }

  /** A generator that generates whole numbers from `start` (inclusive) to `end` (exclusive), counting by `step`. When
    * the end of the sequence is reached, it repeats.
    * @param start
    *   The first number in the sequence
    * @param end
    *   The last number in the sequence (exclusive)
    * @param step
    *   The step to count by
    */
  private[this] case class DoubleSequenceGenerator(start: () => Double, end: () => Double, step: () => Double)
      extends AvroFaker[Double] {
    private var current: Double = start()
    def apply(): Double = {
      lazy val lzyStart = start()
      lazy val lzyEnd = end()
      lazy val lzyStep = step()
      val next = current
      current = current + lzyStep
      if (current >= lzyEnd) current = lzyStart + current - lzyEnd
      next
    }
  }
}

/** A BOOLEAN schema generates random true/false.
  *
  * @param schema
  *   a schema of type BOOLEAN
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BooleanGenerator(schema: Schema, rnd: Random) extends AvroFaker[Boolean] {
  def apply(): Boolean = rnd.nextBoolean()
}

/** A NULL schema generates only null.
  *
  * @param schema
  *   a schema of type NULL
  */
case class NullGenerator(schema: Schema) extends AvroFaker[Void] {
  def apply(): Void = null
}
