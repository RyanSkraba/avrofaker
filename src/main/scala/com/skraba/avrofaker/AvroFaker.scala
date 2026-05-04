package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.math.Numeric.{DoubleIsFractional, IntIsIntegral, LongIsIntegral}
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

  // These are both strategies and arguments
  val StrategyValue: String = "value"
  val StrategyOneOf: String = "oneof"
  val StrategySumOf: String = "sumof"
  val StrategyProductOf: String = "productof"
  val StrategyMinOf: String = "minof"
  val StrategyMaxOf: String = "maxof"
  val StrategyMeanOf: String = "meanof"

  val ArgMin: String = "min"
  val ArgMax: String = "max"
  val ArgMean: String = "mean"
  val ArgStddev: String = "stddev"
  val ArgStep: String = "step"
  val ArgStart: String = "start"
  val ArgIndex: String = "index"

  val ArgLength: String = "length"
  val ArgExpression: String = "faker"

  val ArgToImplicitStrategyMap = ListMap(
    ArgMean -> StrategyGauss,
    ArgStddev -> StrategyGauss,
    ArgStart -> StrategySequence,
    ArgStep -> StrategySequence,
    StrategyValue -> StrategyValue,
    StrategyOneOf -> StrategyOneOf,
    StrategySumOf -> StrategySumOf,
    StrategyProductOf -> StrategyProductOf,
    StrategyMinOf -> StrategyMinOf,
    StrategyMaxOf -> StrategyMaxOf,
    StrategyMeanOf -> StrategyMeanOf
  )

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
      case Schema.Type.FLOAT   => FloatFaker(getArgs(schema))
      case Schema.Type.DOUBLE  => DoubleFaker(getArgs(schema))
      case Schema.Type.BOOLEAN => BooleanGenerator(schema)
      case Schema.Type.NULL    => NullGenerator(schema)
    }
  }

  /** Tests a numeric value and ensures that it is between the bounds (inclusive). */
  def forceBounds[T](value: T, lower: T, upper: T)(implicit num: Numeric[T]): T = {
    import num._
    value min upper max lower
  }

  def toNumOption[T](value: Any)(implicit num: Numeric[T]): Option[T] = {
    val pass1: Option[T] = Option((num, value)).collect {
      case (_, b: Byte)                       => num.fromInt(b)
      case (_, s: Short)                      => num.fromInt(s)
      case (_, i: Int)                        => num.fromInt(i)
      case (_: DoubleIsFractional, l: Long)   => l.toDouble
      case (_: DoubleIsFractional, f: Float)  => f.toDouble
      case (_: DoubleIsFractional, d: Double) => d
      case (_: LongIsIntegral, l: Long)       => l
      case (_: LongIsIntegral, f: Float)      => f.toLong
      case (_: LongIsIntegral, d: Double)     => d.toLong
      case (_: IntIsIntegral, l: Long)        => l.toInt
      case (_: IntIsIntegral, f: Float)       => f.toInt
      case (_: IntIsIntegral, d: Double)      => d.toInt
    }

    pass1.orElse(
      Option((num, value)).flatMap {
        case (dnum: DoubleIsFractional, s: String) => dnum.parseString(s)
        case (lnum: LongIsIntegral, s: String) =>
          lnum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toLong))
        case (inum: IntIsIntegral, s: String) =>
          inum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toInt))
        case _ => None
      }
    )
  }

  def toNum[T](value: Any)(implicit num: Numeric[T]): T = {
    toNumOption(value)(num).getOrElse {
      sys.error(s"Unsupported numeric type: ${num.getClass} for ${value.getClass}")
    }
  }

  def apply[T](args: Map[String, Any], dflts: Map[String, Any] = Map.empty, key: String, bounded: Boolean = true)(
      implicit num: Numeric[T]
  ): AvroFaker[T] = {

    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key).orElse(dflts.get(key))

    // Try to create a constant generator from the argument.
    val constantFn: Option[AvroFaker[T]] = value.flatMap(toNumOption(_)(num)).collect {
      case constant if key == ArgMin || key == ArgMax || !bounded =>
        // Ignore existing when setting the min or max
        ConstantFaker(constant)
      case constant =>
        // Otherwise, a constant value might need to have the bounds applied.
        val minFn = apply[T](args, dflts, ArgMin)
        val maxFn = apply[T](args, dflts, ArgMax)
        (minFn, maxFn) match {
          case (ConstantFaker(min), ConstantFaker(max)) => ConstantFaker(forceBounds(constant, min, max))
          case _                                        => BoundedConstantFaker(constant, minFn, maxFn)
        }
    }
    if (constantFn.nonEmpty) return constantFn.get

    // Get any explicitly matched strategies
    val explicitFn: Option[AvroFaker[T]] = args.get(key) collect {
      case StrategyRandom   => RandomFaker(args, dflts)
      case StrategyGauss    => DoubleGaussFaker(args, dflts)
      case StrategySequence => SequenceFaker[T](args, Map(ArgMin -> 0) ++ dflts)
      case StrategyValue    => AvroFaker(args, dflts, StrategyValue)(num)
      case StrategyOneOf | StrategySumOf | StrategyProductOf | StrategyMinOf | StrategyMaxOf | StrategyMeanOf =>
        AvroFaker(args, dflts, args(key).toString)(num) match {
          case RandomOneOfFaker(fns) =>
            args(key) match {
              case StrategyOneOf     => OneOfFaker(args, fns)
              case StrategySumOf     => SumOfFaker(fns)(num)
              case StrategyProductOf => ProductOfFaker(fns)(num)
              case StrategyMinOf     => MinOfFaker(fns)(num)
              case StrategyMaxOf     => MaxOfFaker(fns)(num)
              case StrategyMeanOf    => MeanOfFaker(fns)(num)

            }
          case other => other
        }
    }
    if (explicitFn.nonEmpty) return explicitFn.get

    // Otherwise try and get a strategy based on the property
    if (!args.contains(key)) {
      val implicitFn: Option[AvroFaker[T]] = ArgToImplicitStrategyMap.keys
        .find(args.contains)
        .map(k => AvroFaker(args ++ Map(key -> ArgToImplicitStrategyMap(k)), dflts, key)(num))
      if (implicitFn.nonEmpty) return implicitFn.get
    }

    value match {
      case Some(m: Map[_, _]) =>
        val mm = m.asInstanceOf[Map[String, Any]]
        if (mm.contains(ArgFaker))
          AvroFaker[T](args.removed(key) ++ m.asInstanceOf[Map[String, Any]], dflts, ArgFaker)
        else
          AvroFaker[T](args.removed(key) ++ m.asInstanceOf[Map[String, Any]], dflts, key)
      case Some(xs: Iterable[_]) =>
        RandomOneOfFaker(xs.map(v => AvroFaker[T](args ++ Map(key -> v), dflts, key)).toSeq)
      case Some(unknown) => throw new IllegalArgumentException(s"Unknown argument content: $unknown")
      case None          => RandomFaker(args, dflts)
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
    if (schema.propsContainsKey(ArgExpression))
      StringFakerGenerator(schema.getProp(ArgExpression))
    else
      StringRandomGenerator(length =
        AvroFaker[Long](
          getArgs(schema),
          Map(ArgMax -> Long.MaxValue, ArgMin -> Long.MinValue, ArgStddev -> 100L, ArgLength -> 10),
          ArgLength
        )
      )

  def apply(ctx: FakerContext): String = fn.apply(ctx)

  /** Generates a random string of the expected length
    *
    * @param length
    *   the size of the string to generate
    */
  private[this] case class StringRandomGenerator(length: FakerContext => Long) extends AvroFaker[String] {
    def apply(ctx: FakerContext): String = ctx.rnd.alphanumeric.take(length(ctx).toInt).mkString
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
    AvroFaker[Long](args, Map(ArgMax -> Int.MaxValue, ArgMin -> Int.MinValue) ++ dflts, ArgFaker)
  def apply(ctx: FakerContext): Int = fn(ctx).toInt
}

/** Generates LONG values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class LongFaker(args: Map[String, Any]) extends AvroFaker[Long] {
  private val fn =
    AvroFaker[Long](args, Map(ArgMax -> Long.MaxValue, ArgMin -> Long.MinValue), ArgFaker)
  def apply(ctx: FakerContext): Long = fn(ctx)
}

/** A faker that returns a single constant value. */
private[this] case class ConstantFaker[T](constant: T) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = constant
}

/** Generates FLOAT values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class FloatFaker(args: Map[String, Any]) extends AvroFaker[Float] {
  private val fn =
    AvroFaker[Double](args, Map(ArgMax -> Double.PositiveInfinity, ArgMin -> Double.NegativeInfinity), key = ArgFaker)
  def apply(ctx: FakerContext): Float = fn(ctx).toFloat
}

/** Generates DOUBLE values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class DoubleFaker(args: Map[String, Any]) extends AvroFaker[Double] {
  private val fn =
    AvroFaker[Double](args, Map(ArgMax -> Double.PositiveInfinity, ArgMin -> Double.NegativeInfinity), key = ArgFaker)
  def apply(ctx: FakerContext): Double = fn(ctx)
}

/** A faker that returns a single constant value bounded by a minimum or a maximum. */
private[this] case class BoundedConstantFaker[T](constant: T, minFn: FakerContext => T, maxFn: FakerContext => T)(
    implicit num: Numeric[T]
) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = forceBounds(constant, minFn(ctx), maxFn(ctx))(num)
}

/** A faker that generates random numbers uniformly from an interval.
  *
  * It can be configured with the following arguments:
  *
  *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
  *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
  */
private[this] case class RandomFaker[T](args: Map[String, Any], dflts: Map[String, Any])(implicit num: Numeric[T])
    extends AvroFaker[T] {
  import num._

  val defaultBounds: Map[String, Any] = num match {
    case _: DoubleIsFractional => Map(ArgMin -> 0.0, ArgMax -> 1.0)
    case _                     => Map.empty
  }

  val min: FakerContext => T = AvroFaker(defaultBounds ++ args, dflts, key = ArgMin)(num)
  val max: FakerContext => T = AvroFaker(defaultBounds ++ args, dflts, key = ArgMax)(num)
  def apply(ctx: FakerContext): T = num match {
    case _: DoubleIsFractional =>
      toNum(ctx.rnd.between(min(ctx).toDouble, max(ctx).toDouble))(num)
    case _: LongIsIntegral =>
      toNum(ctx.rnd.between(min(ctx).toLong, max(ctx).toLong))(num)
    case _ => sys.error(s"Unsupported numeric type: ${num.getClass}")
  }
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
private[this] case class DoubleGaussFaker[T](args: Map[String, Any], dflts: Map[String, Any])(implicit num: Numeric[T])
    extends AvroFaker[T] {

  /** Fractional numbers have a standard deviation of 100 instead of 1 so we see a wider range of values. */
  val defaultStddev: Double = num match {
    case _: DoubleIsFractional => 1.0
    case _                     => 100.0
  }

  val mean = AvroFaker[Double](args, Map(ArgMean -> 0.0d), key = ArgMean, bounded = false)
  val stddev = AvroFaker[Double](args, Map(ArgStddev -> defaultStddev), key = ArgStddev, bounded = false)
  val min = AvroFaker[Double](args, Map(ArgMin -> Double.NegativeInfinity), key = ArgMin)
  val max = AvroFaker[Double](args, Map(ArgMax -> Double.PositiveInfinity), key = ArgMax)

  def apply(ctx: FakerContext): T = {
    val vMin = min(ctx)
    val vMax = max(ctx)
    val vStddev = stddev(ctx)
    val vMean = mean(ctx)
    val next =
      LazyList.continually(ctx.rnd.nextGaussian() * vStddev + vMean).dropWhile(_ < vMin).dropWhile(_ >= vMax).head
    toNum(next)(num)
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
private[this] case class SequenceFaker[T](args: Map[String, Any], dflts: Map[String, Any])(implicit num: Numeric[T])
    extends AvroFaker[T] {
  import num._

  val min: AvroFaker[T] = AvroFaker(Map(ArgMin -> num.zero) ++ args, dflts, ArgMin)
  val max: AvroFaker[T] = AvroFaker(args, Map(ArgMax -> num.zero) ++ dflts, ArgMax)
  val step: AvroFaker[T] = AvroFaker(args, Map(ArgStep -> num.one) ++ dflts, ArgStep, bounded = false)
  private var next: Option[T] = None
  def apply(ctx: FakerContext): T = {

    val vMin = min(ctx)
    val vMax = max(ctx)
    val vStep = step(ctx)

    val generated = next match {
      case None if args.contains(ArgStart) => AvroFaker(args, dflts, ArgStart, bounded = false)(num)(ctx)
      case None                            => if (vStep < num.zero) vMax + vStep else vMin
      case Some(value)                     => value
    }

    next = Some(generated + vStep) // TODO: test overflow

    if (vStep < num.zero && next.exists(_ < vMin)) {
      // If there isn't a specifically specified min in a double countdown, then keep going past zero to
      // maintain a number line.
      if (num.isInstanceOf[LongIsIntegral] || args.contains(ArgMin))
        next = next.map(vMax - vMin + _)
    } else if (vStep > num.zero && next.exists(_ >= vMax))
      next = next.map(vMin - vMax + _)

    generated
  }
}

/** A faker that picks a random strategy from a list. */
private[this] case class RandomOneOfFaker[T](fns: Seq[FakerContext => T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns(ctx.rnd.nextInt(fns.size))(ctx)
}

/** A faker that picks a strategy from a list using an index. */
private[this] case class OneOfFaker[T](args: Map[String, Any], fns: Seq[FakerContext => T]) extends AvroFaker[T] {
  val index: FakerContext => Long = AvroFaker[Long](
    Map(ArgMin -> 0, ArgMax -> fns.size, ArgIndex -> Map { ArgFaker -> StrategyRandom }) ++ args,
    key = ArgIndex,
    bounded = false
  )
  def apply(ctx: FakerContext): T = fns((index(ctx) max 0 min (fns.size - 1)).toInt)(ctx)
}

/** A faker that sums from its list of strategies */
private[this] case class SumOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).sum
}

/** A faker that creates a product from its list of strategies */
private[this] case class ProductOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).product
}

/** A faker that finds the minimum from its list of strategies */
private[this] case class MinOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).min
}

/** A faker that finds the maximum from its list of strategies */
private[this] case class MaxOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns.map(_.apply(ctx)).max
}

/** A faker that finds the mean from its list of strategies */
private[this] case class MeanOfFaker[T](fns: Seq[FakerContext => T])(implicit num: Numeric[T]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = num match {
    case frac: Fractional[_] =>
      import frac._
      fns.map(_.apply(ctx)).sum / frac.fromInt(fns.size)
    case int: Integral[_] =>
      import int._
      fns.map(_.apply(ctx)).sum / int.fromInt(fns.size)
    case _ => throw new IllegalArgumentException("Unsupported numeric type")
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
