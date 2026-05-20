package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.math.Numeric.{DoubleIsFractional, FloatIsFractional, IntIsIntegral, LongIsIntegral}
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

  /** A strategy can be assigned to the [ArgFaker] attribute to explicitly declare how to generate data. */
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
  val StrategyExpression: String = "expression"

  // Arguments are used to configure strategies
  val ArgMin: String = "min"
  val ArgMax: String = "max"
  val ArgMean: String = "mean"
  val ArgStddev: String = "stddev"
  val ArgStep: String = "step"
  val ArgStart: String = "start"
  val ArgIndex: String = "index"
  val ArgLength: String = "length"

  // The original schema is carried along in the map.
  val ArgInternalSchema: String = "__internal_schema"

  // This is the list of arguments that, if present, imply that the given strategy should be used
  val NumberArgToImplicitStrategy = ListMap(
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

  // This is the list of arguments that, if present, imply that the given strategy should be used
  val StringArgToImplicitStrategy = ListMap(
    StrategyValue -> StrategyValue,
    StrategyExpression -> StrategyExpression,
    StrategyOneOf -> StrategyOneOf,
    StrategySumOf -> StrategySumOf,
    ArgLength -> StrategyRandom
  )

  def apply(schema: Schema, args: Map[String, Any]): AvroFaker[_] = {
    schema.getType match {
      case Schema.Type.RECORD  => RecordGenerator(schema)
      case Schema.Type.ENUM    => EnumGenerator(schema)
      case Schema.Type.ARRAY   => ArrayFaker(args ++ getArgs(schema))
      case Schema.Type.MAP     => MapGenerator(schema)
      case Schema.Type.UNION   => UnionGenerator(schema)
      case Schema.Type.FIXED   => FixedGenerator(schema)
      case Schema.Type.STRING  => StringFaker.fake(args ++ getArgs(schema), ArgFaker)
      case Schema.Type.BYTES   => BytesGenerator(schema)
      case Schema.Type.INT     => IntFaker(args ++ getArgs(schema))
      case Schema.Type.LONG    => LongFaker(args ++ getArgs(schema))
      case Schema.Type.FLOAT   => FloatFaker(args ++ getArgs(schema))
      case Schema.Type.DOUBLE  => DoubleFaker(args ++ getArgs(schema))
      case Schema.Type.BOOLEAN => BooleanGenerator(schema)
      case Schema.Type.NULL    => NullGenerator(schema)
    }
  }

  def apply(schema: Schema): AvroFaker[_] = apply(schema, Map.empty)

  def getArgs(in: Schema): Map[String, Any] = {
    def adapt(in: Any): Any = {
      in match {
        case schema: Schema           => adapt(schema.getObjectProps)
        case map: java.util.Map[_, _] => map.asScala.map { case (key, value) => key.toString -> adapt(value) }.toMap
        case array: java.util.Collection[_] => array.asScala.map(adapt)
        case other                          => other
      }
    }
    adapt(in).asInstanceOf[Map[String, Any]] ++ Map(ArgInternalSchema -> in)
  }
}

/** Each AvroFaker creates a type of datum, depending on the schema. */
sealed trait AvroFaker[T] extends (FakerContext => T) {
  def apply(ctx: FakerContext): T
}

/** A faker that returns a single constant value. */
private[this] case class ConstantFaker[T](constant: T) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = constant
}

/** A faker that picks a random strategy from a list. */
private[this] case class RandomOneOfFaker[T](fns: Seq[AvroFaker[T]]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns(ctx.rnd.nextInt(fns.size))(ctx)
}

/** A faker that picks a strategy from a list using an index. */
private[this] case class OneOfFaker[T](indexFn: AvroFaker[Int], fns: Seq[AvroFaker[T]])
    extends AvroFaker[T]
    with NumberFaker {
  def apply(ctx: FakerContext): T = fns((indexFn(ctx) max 0 min (fns.size - 1)).toInt)(ctx)
}

private[this] object OneOfFaker extends NumberFaker {
  def apply[T](args: Map[String, Any], fns: Seq[AvroFaker[T]]): OneOfFaker[T] = {
    OneOfFaker(indexFn = fake[Int]((0, fns.size), args, ArgIndex), fns)
  }
}

/** A RECORD schema generates field data according to the schema of its fields.
  *
  * @param schema
  *   a schema of type RECORD
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
  */
case class EnumGenerator(schema: Schema) extends AvroFaker[String] with NumberFaker {
  private val symbols = schema.getEnumSymbols.asScala.toSeq
  val indexFn: FakerContext => Long = fake[Long]((0L, symbols.size.toLong), getArgs(schema), ArgFaker)
  def apply(ctx: FakerContext): String = symbols(indexFn(ctx).toInt)
}

/** A faker that generates an array delegating to its item type. */
private[this] case class ArrayFaker(lengthFn: AvroFaker[Long], fn: AvroFaker[_]) extends AvroFaker[Array[Any]] {
  def apply(ctx: FakerContext): Array[Any] = Array.fill(lengthFn(ctx).toInt)(fn.apply(ctx))
}

private[this] object ArrayFaker extends NumberFaker {
  def apply(args: Map[String, Any]): ArrayFaker = {
    ArrayFaker(
      lengthFn = fake[Long]((-1, Int.MaxValue), args, ArgLength) match {
        // Here, we replace the undesirable MaxValue random generator with a smaller constant.
        case RandomFaker(ConstantFaker(-1L), ConstantFaker(Int.MaxValue)) => RandomFaker(3L, 6L)
        case other                                                        => other
      },
      fn = AvroFaker(args(ArgInternalSchema).asInstanceOf[Schema].getElementType, args)
    )
  }
}

/** A MAP schema generates 2 to 5 elements of its value type and a 10 character key
  *
  * @param schema
  *   a schema of type MAP
  */
case class MapGenerator(schema: Schema) extends AvroFaker[Map[String, Any]] {
  private val kFn: AvroFaker[String] = StringFaker.fake(Map.empty, ArgFaker)
  private val vFn: AvroFaker[?] = AvroFaker(schema.getValueType)
  def apply(ctx: FakerContext): Map[String, Any] =
    Array.fill(2 + ctx.rnd.nextInt(3))(kFn.apply(ctx) -> vFn.apply(ctx)).toMap
}

/** A UNION schema generates any of its possible schemas with equal probability.
  *
  * @param schema
  *   a schema of type UNION
  */
case class UnionGenerator(schema: Schema) extends AvroFaker[Any] {
  private val fns: Seq[AvroFaker[?]] = schema.getTypes.asScala.map(AvroFaker.apply).toSeq
  def apply(ctx: FakerContext): Any = fns(ctx.rnd.nextInt(fns.size)).apply(ctx)
}

/** A FIXED schema generates a byte array of the expected size.
  *
  * @param schema
  *   a schema of type FIXED
  */
case class FixedGenerator(schema: Schema) extends AvroFaker[Array[Byte]] {
  def apply(ctx: FakerContext): Array[Byte] = ctx.rnd.nextBytes(schema.getFixedSize)
}

/** A faker that generates a random string. */
private[this] case class StringRandomFaker(lengthFn: AvroFaker[Long]) extends AvroFaker[String] {
  def apply(ctx: FakerContext): String = ctx.rnd.alphanumeric.take(lengthFn(ctx).toInt).mkString
}

private[this] object StringRandomFaker extends NumberFaker {
  def apply(args: Map[String, Any]): StringRandomFaker = {
    StringRandomFaker(lengthFn = fake[Long]((-1, Int.MaxValue), args, ArgLength) match {
      // Here, we replace the undesirable MaxValue random generator with a smaller constant.
      case RandomFaker(ConstantFaker(-1L), ConstantFaker(Int.MaxValue)) => ConstantFaker(10L)
      case other                                                        => other
    })
  }
}

/** A faker that uses DataFaker to generate a string. */
private[this] case class StringExpressionFaker(expressionFn: AvroFaker[String]) extends AvroFaker[String] {
  var faker: Option[Faker] = None
  def apply(ctx: FakerContext): String = {
    if (faker.isEmpty) faker = Some(new Faker(new java.util.Random(ctx.rnd.nextLong())))
    faker.get.expression(expressionFn(ctx))
  }
}

private[this] object StringExpressionFaker {
  def apply(args: Map[String, Any]): StringExpressionFaker =
    StringExpressionFaker(expressionFn = StringFaker.fake(args, StrategyExpression, "#{examplify 'A999'}"))
}

/** A faker that concatenates strings. */
private[this] case class StringSumOfFaker(fns: Seq[AvroFaker[_]]) extends AvroFaker[String] {
  def apply(ctx: FakerContext): String = fns.map(_(ctx).toString).mkString("")
}

private[this] object StringFaker {
  def fake(args: Map[String, Any], key: String, dflt: Any = Map(ArgFaker -> StrategyRandom)): AvroFaker[String] = {

    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key)

    // Get any explicitly specified strategy.
    val explicitFn: Option[AvroFaker[String]] = args.get(key) collect {
      case StrategyRandom     => StringRandomFaker(args)
      case StrategyValue      => fake(args, StrategyValue)
      case StrategyExpression => StringExpressionFaker(args)
      case StrategyOneOf | StrategySumOf =>
        fake(args, args(key).toString, dflt) match {
          case RandomOneOfFaker(fns) =>
            args(key) match {
              case StrategyOneOf => OneOfFaker(args, fns)
              case StrategySumOf => StringSumOfFaker(fns)
            }
          case other => other
        }
    }
    if (explicitFn.nonEmpty) return explicitFn.get

    // Otherwise try to create a constant generator from the argument.
    val constantFn: Option[String] = value collect {
      case b: Byte   => b.toString
      case s: Short  => s.toString
      case i: Int    => i.toString
      case l: Long   => l.toString
      case f: Float  => f.toString
      case d: Double => d.toString
      case s: String => s
    }
    if (constantFn.nonEmpty) return ConstantFaker(constantFn.get)

    // Otherwise try and get a strategy based on the property and explicitly specify it.
    if (key == ArgFaker && !args.contains(key)) {
      val implicitFn: Option[Map[String, String]] = StringArgToImplicitStrategy.keys
        .find(args.contains)
        .map(k => Map(key -> StringArgToImplicitStrategy.apply(k)))
      if (implicitFn.nonEmpty) return fake(args ++ implicitFn.get, key, dflt)
    }

    // Otherwise, interpret the strategy depending on its type
    value match {
      case Some(m: Map[_, _]) =>
        // A map, or object, adds attributes to the strategy that can complete how it is interpreted.
        val mm = m.asInstanceOf[Map[String, Any]]
        fake(args.removed(key).removed(ArgFaker) ++ m.asInstanceOf[Map[String, Any]], ArgFaker, dflt)
      case Some(xs: Iterable[_]) =>
        // An array is, by default, a list of strategies that we randomly pick one from.
        RandomOneOfFaker(xs.map(v => fake(args ++ Map(key -> v), key, dflt)).toSeq)
      case None =>
        // We fall back to the default value.
        fake(args ++ Map(key -> dflt), key, 0)
      case Some(unknown) =>
        // This should never occur in our processing
        throw new IllegalArgumentException(s"Unknown argument content: $unknown")
    }
  }
}

/** A BYTES schema generates a byte array between 5 and 10 bytes.
  *
  * @param schema
  *   a schema of type BYTES
  */
case class BytesGenerator(schema: Schema) extends AvroFaker[Array[Byte]] {
  def apply(ctx: FakerContext): Array[Byte] = ctx.rnd.nextBytes(5 + ctx.rnd.nextInt(5))
}

trait NumberFaker {

  /** Tests a numeric value and ensures that it is between the bounds (inclusive). */
  def forceBounds[T](value: T, lower: T, upper: T)(implicit num: Numeric[T]): T = {
    import num._
    value min upper max lower
  }

  /** For the numbers that we handle, convert them to a numeric type that we wish to use during generation. Note that we
    * only use Doubles and Longs, which are set to the bounds or precision of Int and Float at the last minute, but we
    * handle Integers just in case they are explicitly set.
    */
  def toNumOption[T](value: Any)(implicit num: Numeric[T]): Option[T] = {
    val pass1: Option[Any] = Option((num, value)).collect {
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

    pass1
      .orElse(
        Option((num, value)).flatMap {
          case (dnum: DoubleIsFractional, s: String) => dnum.parseString(s)
          case (lnum: LongIsIntegral, s: String) =>
            lnum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toLong))
          case (inum: IntIsIntegral, s: String) =>
            inum.parseString(s).orElse(DoubleIsFractional.parseString(s).map(_.toInt))
          case _ => None
        }
      )
      .asInstanceOf[Option[T]]
  }

  def toNum[T](value: Any)(implicit num: Numeric[T]): T = {
    toNumOption(value)(num).getOrElse {
      sys.error(s"Unsupported numeric type: ${num.getClass} for ${value.getClass}")
    }
  }

  def fake[T](bounds: (T, T), args: Map[String, Any], key: String, dflt: Any = Map(ArgFaker -> StrategyRandom))(implicit
      num: Numeric[T]
  ): AvroFaker[T] = {

    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key)

    // Shortcut defaults for the bounds
    if (key == ArgMin && value.isEmpty) return ConstantFaker(bounds._1)
    if (key == ArgMax && value.isEmpty) return ConstantFaker(bounds._2)

    // Get any explicitly specified strategy.
    val explicitFn: Option[AvroFaker[T]] = args.get(key) collect {
      case StrategyRandom   => RandomFaker(bounds, args)
      case StrategyGauss    => DoubleGaussFaker(bounds, args)
      case StrategySequence => SequenceFaker[T](bounds, args)
      case StrategyValue    => BoundedFaker(bounds, args)
      case StrategyOneOf | StrategySumOf | StrategyProductOf | StrategyMinOf | StrategyMaxOf | StrategyMeanOf =>
        fake(bounds, args, args(key).toString, dflt)(num) match {
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

    // Otherwise try to create a constant generator from the argument.
    val constantFn: Option[AvroFaker[T]] = value.flatMap(toNumOption(_)(num)).collect { ConstantFaker(_) }
    if (constantFn.nonEmpty) return constantFn.get

    // Otherwise try and get a strategy based on the property and explicitly specify it.
    if (key == ArgFaker && !args.contains(key)) {
      val implicitFn: Option[Map[String, String]] = NumberArgToImplicitStrategy.keys
        .find(args.contains)
        .map(k => Map(key -> NumberArgToImplicitStrategy.apply(k)))
      if (implicitFn.nonEmpty) return fake(bounds, args ++ implicitFn.get, key, dflt)(num)
    }

    // Otherwise, interpret the strategy depending on its type
    value match {
      case Some(m: Map[_, _]) =>
        // A map, or object, adds attributes to the strategy that can complete how it is interpreted.
        val mm = m.asInstanceOf[Map[String, Any]]
        fake[T](bounds, args.removed(key).removed(ArgFaker) ++ m.asInstanceOf[Map[String, Any]], ArgFaker, dflt)
      case Some(xs: Iterable[_]) =>
        // An array is, by default, a list of strategies that we randomly pick one from.
        RandomOneOfFaker(xs.map(v => fake[T](bounds, args ++ Map(key -> v), key, dflt)).toSeq)
      case None =>
        // We fall back to the default value.
        fake[T](bounds, args ++ Map(key -> dflt), key, 0)
      case Some(unknown) =>
        // This should never occur in our processing
        throw new IllegalArgumentException(s"Unknown argument content: $unknown")
    }
  }
}

/** Generates INT values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  * @param dflts
  *   If an argument is optional and not in the configuration, the value to use.
  */
case class IntFaker(args: Map[String, Any]) extends AvroFaker[Int] with NumberFaker {
  private val fn = fake[Long]((Int.MinValue.toLong, Int.MaxValue.toLong), args, ArgFaker)
  def apply(ctx: FakerContext): Int = fn(ctx).toInt
}

/** Generates LONG values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class LongFaker(args: Map[String, Any]) extends AvroFaker[Long] with NumberFaker {
  private val fn = fake[Long]((Long.MinValue, Long.MaxValue), args, ArgFaker)
  def apply(ctx: FakerContext): Long = fn(ctx)
}

/** Generates FLOAT values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class FloatFaker(args: Map[String, Any]) extends AvroFaker[Float] with NumberFaker {
  private val fn = fake[Double]((Float.NegativeInfinity.toDouble, Float.PositiveInfinity.toDouble), args, ArgFaker)
  def apply(ctx: FakerContext): Float = fn(ctx).toFloat
}

/** Generates DOUBLE values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class DoubleFaker(args: Map[String, Any]) extends AvroFaker[Double] with NumberFaker {
  private val fn = fake[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgFaker)
  def apply(ctx: FakerContext): Double = fn(ctx)
}

/** A faker that returns a value bounded by a minimum or a maximum. */
private[this] case class BoundedFaker[T](minFn: AvroFaker[T], maxFn: AvroFaker[T], fn: AvroFaker[T])(implicit
    num: Numeric[T]
) extends AvroFaker[T]
    with NumberFaker {
  def apply(ctx: FakerContext): T = forceBounds(fn(ctx), minFn(ctx), maxFn(ctx))
}

private[this] object BoundedFaker extends NumberFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): BoundedFaker[T] =
    BoundedFaker(
      minFn = fake(bounds, args, ArgMin)(num),
      maxFn = fake(bounds, args, ArgMax)(num),
      fn = fake(bounds, args, StrategyValue)(num)
    )
}

/** A faker that generates random numbers uniformly from an interval.
  *
  * It can be configured with the following arguments:
  *
  *   - `min`: The lower bound (inclusive) of the sequence (Default: 0).
  *   - `max`: The upper bound (exclusive) of the sequence (No default, this must be supplied).
  */
private[this] case class RandomFaker[T](minFn: AvroFaker[T], maxFn: AvroFaker[T])(implicit num: Numeric[T])
    extends AvroFaker[T]
    with NumberFaker {
  import num._
  def apply(ctx: FakerContext): T = num match {
    case _: FloatIsFractional =>
      toNum(ctx.rnd.between(minFn(ctx).toFloat, maxFn(ctx).toFloat))(num)
    case _: DoubleIsFractional =>
      toNum(ctx.rnd.between(minFn(ctx).toDouble, maxFn(ctx).toDouble))(num)
    case _: LongIsIntegral =>
      toNum(ctx.rnd.between(minFn(ctx).toLong, maxFn(ctx).toLong))(num)
    case _: IntIsIntegral =>
      toNum(ctx.rnd.between(minFn(ctx).toInt, maxFn(ctx).toInt))(num)
    case _ => sys.error(s"Unsupported numeric type: ${num.getClass}")
  }
}

private[this] object RandomFaker extends NumberFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): RandomFaker[T] = {
    val defaultBounds: (T, T) = num match {
      case _: Fractional[T] => (num.zero, num.one)
      case _                => bounds
    }
    RandomFaker(minFn = fake(defaultBounds, args, ArgMin), maxFn = fake(defaultBounds, args, ArgMax))
  }
  def apply[T](min: T, max: T)(implicit num: Numeric[T]): RandomFaker[T] = {
    RandomFaker(minFn = ConstantFaker[T](min), maxFn = ConstantFaker[T](max))
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
private[this] case class DoubleGaussFaker[T](
    minFn: AvroFaker[Double],
    maxFn: AvroFaker[Double],
    meanFn: AvroFaker[Double],
    stddevFn: AvroFaker[Double]
)(implicit num: Numeric[T])
    extends AvroFaker[T]
    with NumberFaker {
  def apply(ctx: FakerContext): T = {
    val min = minFn(ctx)
    val max = maxFn(ctx)
    val mean = meanFn(ctx)
    val stddev = stddevFn(ctx)
    val next = LazyList.continually(ctx.rnd.nextGaussian() * stddev + mean).dropWhile(_ < min).dropWhile(_ >= max).head
    toNum(next)(num)
  }
}

private[this] object DoubleGaussFaker extends NumberFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): DoubleGaussFaker[T] = {

    /** Fractional numbers have a standard deviation of 100 instead of 1 so we see a wider range of values. */
    val defaultStddev: Double = num match {
      case _: Fractional[T] => 1.0
      case _                => 100.0
    }

    DoubleGaussFaker[T](
      minFn = fake[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMin, bounds._1),
      maxFn = fake[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMax, bounds._2),
      meanFn = fake[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMean, 0),
      stddevFn = fake[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgStddev, defaultStddev)
    )
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
  *   - `start`: The starting value for the sequence. (Default: depends on the step, whether it starts from the 0, upper
  *     or lower bound.)
  */
private[this] case class SequenceFaker[T](
    minFn: AvroFaker[T],
    maxFn: AvroFaker[T],
    stepFn: AvroFaker[T],
    startFn: Option[AvroFaker[T]],
    overridesMin: Boolean
)(implicit num: Numeric[T])
    extends AvroFaker[T]
    with NumberFaker {
  import num._
  private var next: Option[T] = None
  def apply(ctx: FakerContext): T = {

    // The direction of the next step
    val min = minFn(ctx)
    val max = maxFn(ctx)
    val step = stepFn(ctx)

    val generated = next match {
      case None if startFn.nonEmpty => startFn.get(ctx)
      case None if step < num.zero  => max + step
      case None if !overridesMin    => num.zero
      case None                     => min
      case Some(value)              => value
    }

    next = Some(generated + step) // TODO: test overflow

    if (step < num.zero && next.exists(_ < min)) {
      next = next.map(max - min + _)
    } else if (step > num.zero && next.exists(_ >= max))
      next = next.map(min - max + _)

    generated
  }
}

private[this] object SequenceFaker extends NumberFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): SequenceFaker[T] = {
    SequenceFaker(
      minFn = fake[T](bounds, args, ArgMin),
      maxFn = fake[T](bounds, args, ArgMax),
      stepFn = fake[T](bounds, args, ArgStep, 1),
      startFn = if (args.contains(ArgStart)) Some(fake[T](bounds, args, ArgStart)(num)) else None,
      overridesMin = args.contains(ArgMin)
    )
  }
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
