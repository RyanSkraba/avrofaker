package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

/** The context used to generate a new data. */
case class FakerContext(rnd: Random = new Random())

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

  val ArgFaker: String = "faker"

  val StrategyRandom: String = "random"
  val StrategyGauss: String = "gauss"
  val StrategySequence: String = "sequence"
  val StrategyValue: String = "value"

  val ArgMin: String = "min"
  val ArgMax: String = "max"
  val ArgMean: String = "mean"
  val ArgStddev: String = "stddev"
  val ArgStep: String = "step"
  val ArgStart: String = "start"
  val ArgValue: String = "value"

  val PropLength: String = "length"
  val PropFaker: String = "faker"

  def apply(schema: Schema): AvroFaker[_] = {
    schema.getType match {
      case Schema.Type.RECORD  => RecordGenerator(schema)
      case Schema.Type.ENUM    => EnumGenerator(schema)
      case Schema.Type.ARRAY   => ArrayGenerator(schema)
      case Schema.Type.MAP     => MapGenerator(schema)
      case Schema.Type.UNION   => UnionGenerator(schema)
      case Schema.Type.FIXED   => FixedGenerator(schema)
      case Schema.Type.STRING  => StringGenerator(schema)
      case Schema.Type.BYTES   => BytesGenerator(schema)
      case Schema.Type.INT     => IntFaker(getArgs(schema))
      case Schema.Type.LONG    => LongFaker(getArgs(schema))
      case Schema.Type.FLOAT   => FloatGenerator(schema)
      case Schema.Type.DOUBLE  => DoubleGenerator(schema)
      case Schema.Type.BOOLEAN => BooleanGenerator(schema)
      case Schema.Type.NULL    => NullGenerator(schema)
    }
  }

  def getArgs(in: Schema): Map[String, Any] = {
    def adapt(in: Any): Any = {
      in match {
        case schema: Schema           => adapt(schema.getObjectProps)
        case map: java.util.Map[_, _] => map.asScala.map { case (key, value) => key.toString -> adapt(value) }.toMap
        case array: java.util.Collection[_] => array.asScala.map(adapt)
        case other                          => other
      }
    }
    adapt(in).asInstanceOf[Map[String, Any]]
  }

  /** An adapter that turns a schema and its annotations into a partial function. */
  def getPropsFn(schema: Schema): PartialFunction[String, Any] =
    new PartialFunction[String, Any]() {
      override def isDefinedAt(key: String): Boolean = schema.propsContainsKey(key)
      override def apply(key: String): Any = schema.getObjectProp(key)
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
sealed trait AvroFaker[T] extends (FakerContext => T) {
  def apply(ctx: FakerContext): T
}

/** A RECORD schema generates field data according to the schema of its fields.
  *
  * @param schema
  *   a schema of type RECORD
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class RecordGenerator(schema: Schema) extends AvroFaker[GenericRecord] {
  private val fn: Map[Field, AvroFaker[?]] =
    schema.getFields.asScala.map((f: Field) => f -> AvroFaker(f.schema())).toMap
  def apply(ctx: FakerContext): GenericRecord = {
    val rb = new GenericRecordBuilder(schema)
    fn.map { case (f, fgen) => rb.set(f, fgen.apply(ctx)) }
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
case class EnumGenerator(schema: Schema) extends AvroFaker[String] {
  private val symbols = schema.getEnumSymbols.asScala.toSeq
  private val indexFn = IntFaker(getArgs(schema), dflts = Map(ArgMin -> 0, ArgMax -> symbols.size))
  def apply(ctx: FakerContext): String = symbols(indexFn(ctx))
}

/** An ARRAY schema generates 2 to 5 elements of its element type
  *
  * @param schema
  *   a schema of type ARRAY
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class ArrayGenerator(schema: Schema) extends AvroFaker[Array[Any]] {
  private val fn: AvroFaker[?] = AvroFaker(schema.getElementType)
  def apply(ctx: FakerContext): Array[Any] = Array.fill(2 + ctx.rnd.nextInt(3))(fn.apply(ctx))
}

/** A MAP schema generates 2 to 5 elements of its value type and a 10 character key
  *
  * @param schema
  *   a schema of type MAP
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class MapGenerator(schema: Schema) extends AvroFaker[Map[String, Any]] {
  private val kFn: StringGenerator = StringGenerator(schema)
  private val vFn: AvroFaker[?] = AvroFaker(schema.getValueType)
  def apply(ctx: FakerContext): Map[String, Any] =
    Array.fill(2 + ctx.rnd.nextInt(3))(kFn.apply(ctx) -> vFn.apply(ctx)).toMap
}

/** A UNION schema generates any of its possible schemas with equal probability.
  *
  * @param schema
  *   a schema of type UNION
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class UnionGenerator(schema: Schema) extends AvroFaker[Any] {
  private val fns: Seq[AvroFaker[?]] = schema.getTypes.asScala.map(AvroFaker.apply).toSeq
  def apply(ctx: FakerContext): Any = fns(ctx.rnd.nextInt(fns.size)).apply(ctx)
}

/** A FIXED schema generates a byte array of the expected size.
  *
  * @param schema
  *   a schema of type FIXED
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FixedGenerator(schema: Schema) extends AvroFaker[Array[Byte]] {
  def apply(ctx: FakerContext): Array[Byte] = ctx.rnd.nextBytes(schema.getFixedSize)
}

/** A STRING schema generator
  *
  * @param schema
  *   a schema of type STRING
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class StringGenerator(schema: Schema) extends AvroFaker[String] {
  private val fn =
    if (schema.propsContainsKey(PropFaker))
      StringFakerGenerator(schema.getProp(PropFaker))
    else
      StringRandomGenerator(length = getLong(schema, PropLength, 10, PartialFunction.empty))

  def apply(ctx: FakerContext): String = fn.apply(ctx)

  /** Generates a random string of the expected length
    *
    * @param length
    *   the size of the string to generate
    */
  private[this] case class StringRandomGenerator(length: () => Long) extends AvroFaker[String] {
    def apply(ctx: FakerContext): String = ctx.rnd.alphanumeric.take(length().toInt).mkString
  }

  /** Uses Faker to generate a string
    *
    * @param expression
    *   the Faker expression to generate
    */
  private[this] case class StringFakerGenerator(expression: String) extends AvroFaker[String] {
    var faker: Option[Faker] = None
    def apply(ctx: FakerContext): String = {
      if (faker.isEmpty) faker = Some(new Faker(new java.util.Random(ctx.rnd.nextLong())))
      faker.get.expression(expression)
    }
  }
}

/** A BYTES schema generates a byte array between 5 and 10 bytes.
  *
  * @param schema
  *   a schema of type BYTES
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BytesGenerator(schema: Schema) extends AvroFaker[Array[Byte]] {
  def apply(ctx: FakerContext): Array[Byte] = ctx.rnd.nextBytes(5 + ctx.rnd.nextInt(5))
}

/** Generates INT values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  * @param dflts
  *   If an argument is optional and not in the configuration, the value to use.
  */
case class IntFaker(args: Map[String, Any], dflts: Map[String, Any] = Map.empty) extends AvroFaker[Int] {
  private val fn =
    LongFaker.getFaker(args, Map(ArgMax -> Int.MaxValue, ArgMin -> Int.MinValue, ArgStddev -> 100L) ++ dflts)
  def apply(ctx: FakerContext): Int = fn(ctx).toInt
}

/** Generates LONG values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class LongFaker(args: Map[String, Any]) extends AvroFaker[Long] {
  private val fn = LongFaker.getFaker(args, Map(ArgMax -> Long.MaxValue, ArgMin -> Long.MinValue, ArgStddev -> 100L))
  def apply(ctx: FakerContext): Long = fn(ctx)
}

object LongFaker {

  def getLong(args: Map[String, Any], dflts: Map[String, Any] = Map.empty, key: String): FakerContext => Long = {
    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key).orElse(dflts.get(key))
    // If it's a constant, create a generator that only returns that constant.
    value.flatMap {
      case b: Byte   => Some(b.toLong)
      case s: Short  => Some(s.toLong)
      case i: Int    => Some(i.toLong)
      case l: Long   => Some(l)
      case f: Float  => Some(f.toLong)
      case d: Double => Some(d.toLong)
      case s: String => Some(Try(s.toLong).orElse(Try(s.toDouble.toLong)).get)
      case _         => None
    } match {
      case Some(constant: Long) if key == ArgMin || key == ArgMax =>
        // Ignore existing when setting the min or max
        ConstantFaker(constant)
      case Some(l: Long) =>
        // Otherwise, a constant value might need to have the bounds applied.
        val minFn = getLong(args, dflts, ArgMin)
        val maxFn = getLong(args, dflts, ArgMax)
        (minFn, maxFn) match {
          case (ConstantFaker(min), ConstantFaker(max)) => ConstantFaker(l min max max min)
          case _                                        => ctx: FakerContext => l min maxFn(ctx) max minFn(ctx)
        }
      case None =>
        value match {
          case Some(m: Map[_, _]) => getFaker(args.removed(key) ++ m.asInstanceOf[Map[String, Any]], dflts)
          case Some(xs: Iterable[_]) =>
            val fns = xs.map(v => getFaker(args ++ Map(key -> v), dflts, key)).toSeq
            ctx => fns(ctx.rnd.nextInt(fns.size))(ctx)
          case Some(unknown) => throw new IllegalArgumentException(s"Unknown argument content: $unknown")
          case None          => LongRandomFaker(dflts ++ args)
        }
    }
  }

  def getFaker(
      args: Map[String, Any],
      dflts: Map[String, Any] = Map.empty,
      key: String = ArgFaker
  ): FakerContext => Long = {
    args.get(key) match {
      case Some(StrategyRandom)   => LongRandomFaker(dflts ++ args)
      case Some(StrategyGauss)    => ctx => DoubleGaussFaker(dflts ++ args)(ctx).toLong
      case Some(StrategySequence) => SequenceFaker[Long](dflts ++ Map(ArgMin -> 0) ++ args)
      case Some(StrategyValue)    => getFaker(args, dflts, ArgValue)
      case None if args.contains(ArgMean) || args.contains(ArgStddev) =>
        getFaker(args ++ Map(key -> StrategyGauss), dflts, key)
      case None if args.contains(ArgStart) || args.contains(ArgStep) =>
        getFaker(args ++ Map(key -> StrategySequence), dflts, key)
      case None if args.contains(ArgValue) =>
        getFaker(args, dflts, ArgValue)
        getFaker(args ++ Map(key -> StrategyValue), dflts, key)
      case _ =>
        getLong(args, dflts, key)
    }
  }
}

/** A */
private[this] case class ConstantFaker[T](constant: T) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = constant
}

/** A faker that generates random numbers uniformly from an interval.
  *
  * It can be configured with the following arguments:
  *
  *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
  *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
  */
private[this] case class LongRandomFaker(args: Map[String, Any]) extends AvroFaker[Long] {
  val min: FakerContext => Long = LongFaker.getLong(args, key = ArgMin)
  val max: FakerContext => Long = LongFaker.getLong(args, key = ArgMax)
  def apply(ctx: FakerContext): Long = ctx.rnd.between(min(ctx), max(ctx))
}

/** A faker that generates numbers along the Gauss distribution to have a bell curve nicely centered around a median.
  *
  * It can be configured with the following arguments:
  *
  *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
  *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
  *   - `mean` and `stddev`: The mean and standard deviation to position the bell curve (Default: 0.0 and 1.0 for
  *     DOUBLE)
  *
  * Values outside the `min` and `max` are discarded. Be careful, if your valid interval is rarely drawn from a gaussian
  * distribution, this generator might be very slow to generate value.
  */
private[this] case class DoubleGaussFaker(args: Map[String, Any]) extends AvroFaker[Double] {
  val mean: () => Double = () => args.get(ArgMean).map(_.toString.toDouble).getOrElse(0.0d)
  val stddev: () => Double = () => args.get(ArgStddev).map(_.toString.toDouble).getOrElse(1.0d)
  val min: () => Double = () => args.get(ArgMin).map(_.toString.toDouble).getOrElse(Double.NegativeInfinity)
  val max: () => Double = () => args.get(ArgMax).map(_.toString.toDouble).getOrElse(Double.PositiveInfinity)
  def apply(ctx: FakerContext): Double = {
    lazy val vMin = min()
    lazy val vMax = max()
    lazy val vStddev = stddev()
    lazy val vMean = mean()
    LazyList.continually(ctx.rnd.nextGaussian() * vStddev + vMean).dropWhile(_ < vMin).dropWhile(_ >= vMax).head
  }
}

/** A faker that generates whole numbers within an interval, moving by a step every time. When the end of the sequence
  * is reached, it wraps around and repeats.
  *
  * It can be configured with the following arguments:
  *
  *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
  *   - `max`: The upper bound (exclusive) of the sequence (Default: Long.MaxValue for LONG, etc, depending on the
  *     type).
  *   - `step`: The distance to move in the interval (Default: 1)
  *   - `start`: The starting value for the sequence. (Default: depends on the step, whether it starts from the upper or
  *     lower bound.)
  */
private[this] case class SequenceFaker[T](args: Map[String, Any])(implicit num: Numeric[T]) extends AvroFaker[T] {
  import num._
  val min: () => T = () => args.get(ArgMin).map(_.toString).flatMap(num.parseString).getOrElse(num.zero)
  val max: () => T = () => args.get(ArgMax).map(_.toString).flatMap(num.parseString).getOrElse(num.zero)
  val step: () => T = () => args.get(ArgStep).map(_.toString).flatMap(num.parseString).getOrElse(num.one)
  private var next: Option[T] = args.get(ArgStart).map(_.toString).flatMap(num.parseString).map(_ max min() min max())
  def apply(ctx: FakerContext): T = {
    val vMin = min()
    val vMax = max()
    val vStep = step()

    val generated = next.getOrElse(if (vStep < num.zero) vMax + vStep else vMin)

    next = Some(generated + vStep) // TODO: test overflow

    if (vStep < num.zero && next.exists(_ < vMin))
      next = next.map(vMax - vMin + _)
    else if (vStep > num.zero && next.exists(_ >= vMax))
      next = next.map(vMin - vMax + _)

    generated
  }
}

/** A FLOAT schema generates a random floating point number
  *
  * @param schema
  *   a schema of type FLOAT
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class FloatGenerator(schema: Schema) extends AvroFaker[Float] {
  private val internalGen = DoubleGenerator(schema)
  def apply(ctx: FakerContext): Float = internalGen(ctx).toFloat
}

/** A DOUBLE schema generates a random floating point number
  *
  * @param schema
  *   a schema of type DOUBLE
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class DoubleGenerator(schema: Schema) extends AvroFaker[Double] {
  private val fn =
    if (schema.propsContainsKey(ArgMean) || schema.propsContainsKey(ArgStddev))
      DoubleGaussGenerator(
        mean = getDouble(schema, ArgMean, 0),
        stdDev = getDouble(schema, ArgStddev, 1)
      )
    else if (schema.propsContainsKey(ArgStart) || schema.propsContainsKey(ArgStep))
      SequenceFaker[Double](Map(ArgMax -> Double.MaxValue, ArgMin -> Double.MinValue) ++ getArgs(schema))
    else
      DoubleRandomGenerator(
        min = getDouble(schema, ArgMin, 0),
        max = getDouble(schema, ArgMax, 1)
      )

  def apply(ctx: FakerContext): Double = fn(ctx)

  /** Generates a random double
    *
    * @param min
    *   The smallest number to be generated, or the lower limit.
    * @param max
    *   The upper limit (exclusive)
    */
  private[this] case class DoubleRandomGenerator(min: () => Double, max: () => Double) extends AvroFaker[Double] {
    def apply(ctx: FakerContext): Double = ctx.rnd.between(min(), max())
  }

  /** Generates a double along the gaussian distribution */
  private[this] case class DoubleGaussGenerator(mean: () => Double, stdDev: () => Double) extends AvroFaker[Double] {
    def apply(ctx: FakerContext): Double = ctx.rnd.nextGaussian() * stdDev() + mean()
  }
}

/** A BOOLEAN schema generates random true/false.
  *
  * @param schema
  *   a schema of type BOOLEAN
  * @param rnd
  *   random number generator (for reproducibility if desired)
  */
case class BooleanGenerator(schema: Schema) extends AvroFaker[Boolean] {
  def apply(ctx: FakerContext): Boolean = ctx.rnd.nextBoolean()
}

/** A NULL schema generates only null.
  *
  * @param schema
  *   a schema of type NULL
  */
case class NullGenerator(schema: Schema) extends AvroFaker[Void] {
  def apply(ctx: FakerContext): Void = null
}
