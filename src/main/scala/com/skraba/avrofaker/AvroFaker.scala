package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.avro.generic.{GenericRecordBuilder, IndexedRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap
import scala.math.Numeric._
import scala.util.Random

/** A context used to set up the generator. This is used internally.
  * @param schema
  *   The schema that we're currently generating data for.
  * @param parentArgs
  *   Arguments and strategies inherited from the parents.
  * @param asJava
  *   True if we should generate Java data (the default), false for Scala data. This mainly affects the collection type
  *   MAP and ARRAY, but also has an impact on ENUM, FIXED, BYTES. Notably, STRING data in Java will be the Avro Utf8
  *   type. The scala versions are useful for testing and using in Scala code.
  * @param rnd
  *   A random number generator that can set with a rpredefined key for reproducibility.
  */
private case class SetupContext(schema: Schema, parentArgs: Map[String, Any], asJava: Boolean, rnd: Random) {
  lazy val args: Map[String, Any] = parentArgs ++ argsOf(schema)

  def argsOf(in: Any): Map[String, Any] = {
    def adapt(in: Any): Any = {
      in match {
        case schema: Schema           => adapt(schema.getObjectProps)
        case field: Schema.Field      => adapt(field.getObjectProps)
        case map: java.util.Map[_, _] => map.asScala.map { case (key, value) => key.toString -> adapt(value) }.toMap
        case array: java.util.Collection[_] => array.asScala.map(adapt)
        case other                          => other
      }
    }
    adapt(in).asInstanceOf[Map[String, Any]]
  }
}

/** The context used to generate a new data.
  * @param rnd
  *   A random number generator that can set with a rpredefined key for reproducibility.
  */
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
  val StrategyWeights: String = "weights"
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
  val ArgKey: String = "key"

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
    StrategyMeanOf -> StrategyMeanOf,
    StrategyWeights -> StrategyWeights
  )

  // This is the list of arguments that, if present, imply that the given strategy should be used
  val StringArgToImplicitStrategy = ListMap(
    StrategyValue -> StrategyValue,
    StrategyExpression -> StrategyExpression,
    StrategyOneOf -> StrategyOneOf,
    StrategySumOf -> StrategySumOf,
    ArgLength -> StrategyRandom
  )

  def apply(setup: SetupContext): AvroFaker[_] = {
    setup.schema.getType match {
      case Schema.Type.RECORD => RecordFaker(setup)
      case Schema.Type.ENUM =>
        val faker = {
          val fns = setup.schema.getEnumSymbols.asScala.toSeq.map(ConstantFaker[String])
          val indexFn = NumberFaker[Int]((0, fns.size), setup.args, ArgIndex)
          OneOfFaker(indexFn, fns)
        }
        if (setup.asJava) new AvroFaker[EnumSymbol] {
          override def apply(ctx: FakerContext): EnumSymbol = new EnumSymbol(setup.schema, faker(ctx))
        }
        else faker
      case Schema.Type.ARRAY => ArrayFaker(setup)
      case Schema.Type.MAP   => MapFaker(setup)
      case Schema.Type.UNION =>
        val fns = setup.schema.getTypes.asScala.toSeq
          .map(schema => apply(setup.copy(schema = schema)))
          .map(_.asInstanceOf[AvroFaker[Any]])
        val indexFn = NumberFaker[Int](
          (0, fns.size),
          setup.args ++ setup.schema.getTypes.asScala.headOption
            .flatMap(schema => Option(setup.argsOf(schema.getObjectProps.get("union"))))
            .getOrElse(Map.empty[String, Any]),
          ArgIndex
        )
        OneOfFaker[Any](indexFn, fns)
      case Schema.Type.FIXED   => BytesFaker(setup)
      case Schema.Type.STRING  => StringFaker(setup, ArgFaker)
      case Schema.Type.BYTES   => BytesFaker(setup)
      case Schema.Type.INT     => IntFaker(setup.args)
      case Schema.Type.LONG    => LongFaker(setup.args)
      case Schema.Type.FLOAT   => FloatFaker(setup.args)
      case Schema.Type.DOUBLE  => DoubleFaker(setup.args)
      case Schema.Type.BOOLEAN => BooleanFaker(setup.args)
      case Schema.Type.NULL    => NullFaker
    }
  }

  def apply(schema: Schema): AvroFaker[_] = apply(SetupContext(schema, Map.empty, asJava = true, new Random()))
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
private[this] case class OneOfFaker[T](indexFn: AvroFaker[Int], fns: Seq[AvroFaker[T]]) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = fns(indexFn(ctx) max 0 min (fns.size - 1))(ctx)
}

private[this] object OneOfFaker {
  def apply[T](args: Map[String, Any], fns: Seq[AvroFaker[T]]): OneOfFaker[T] = {
    OneOfFaker(indexFn = NumberFaker[Int]((0, fns.size), args, ArgIndex), fns)
  }
}

/** A faker that generates field data according to the schema of its fields. */
private[this] case class RecordFaker(schema: Schema, fns: Seq[(Field, AvroFaker[_])]) extends AvroFaker[IndexedRecord] {
  def apply(ctx: FakerContext): IndexedRecord = {
    val rb = new GenericRecordBuilder(schema)
    fns.map { case (f, fgen) => rb.set(f, fgen.apply(ctx)) }
    rb.build()
  }
}

private[this] object RecordFaker {
  def apply(setup: SetupContext): RecordFaker = {
    RecordFaker(
      schema = setup.schema,
      fns = setup.schema.getFields.asScala
        .map((f: Field) => f -> AvroFaker(setup.copy(schema = f.schema(), parentArgs = setup.args ++ setup.argsOf(f))))
        .toSeq
    )
  }
}

/** A faker that generates an array delegating to its item type. */
private[this] case class ArrayFaker(lengthFn: AvroFaker[Long], fn: AvroFaker[_]) extends AvroFaker[Seq[Any]] {
  def apply(ctx: FakerContext): Seq[Any] = Seq.fill(lengthFn(ctx).toInt)(fn.apply(ctx))
}

private[this] object ArrayFaker {
  def apply(setup: SetupContext): AvroFaker[_] = {
    val faker = ArrayFaker(
      lengthFn = NumberFaker[Long]((-2, Int.MaxValue), setup.args, ArgLength) match {
        // Here, we replace the undesirable MaxValue random generator with a smaller constant.
        case RandomFaker(ConstantFaker(-2L), ConstantFaker(Int.MaxValue)) => RandomFaker(3L, 6L)
        case other                                                        => other
      },
      fn = AvroFaker(setup.copy(schema = setup.schema.getElementType, parentArgs = setup.args))
    )
    if (setup.asJava) new AvroFaker[java.util.List[Any]] {
      override def apply(ctx: FakerContext): java.util.List[Any] = faker(ctx).asJava
    }
    else faker
  }
}

/** A faker that generates an array delegating to its item type. */
private[this] case class MapFaker(lengthFn: AvroFaker[Long], keyFn: AvroFaker[_], fn: AvroFaker[_])
    extends AvroFaker[Map[Any, Any]] {
  def apply(ctx: FakerContext): Map[Any, Any] = Array.fill(lengthFn(ctx).toInt)(keyFn(ctx) -> fn.apply(ctx)).toMap
}

private[this] object MapFaker {
  def apply(setup: SetupContext): AvroFaker[_] = {
    val faker = MapFaker(
      lengthFn = NumberFaker[Long]((-1, Int.MaxValue), setup.args, ArgLength) match {
        // Here, we replace the undesirable MaxValue random generator with a smaller constant.
        case RandomFaker(ConstantFaker(-1L), ConstantFaker(Int.MaxValue)) => RandomFaker(3L, 6L)
        case other                                                        => other
      },
      keyFn = StringFaker(setup, ArgKey),
      fn = AvroFaker(setup.copy(schema = setup.schema.getValueType, parentArgs = setup.args))
    )
    if (setup.asJava) new AvroFaker[java.util.Map[Any, Any]] {
      override def apply(ctx: FakerContext): java.util.Map[Any, Any] = faker(ctx).asJava
    }
    else faker
  }
}

/** A faker that generates a random string. */
private[this] case class StringRandomFaker(lengthFn: AvroFaker[Long]) extends AvroFaker[String] {
  def apply(ctx: FakerContext): String = ctx.rnd.alphanumeric.take(lengthFn(ctx).toInt).mkString
}

private[this] object StringRandomFaker {
  def apply(args: Map[String, Any]): StringRandomFaker = {
    StringRandomFaker(lengthFn = NumberFaker[Long]((-1, Int.MaxValue), args, ArgLength) match {
      // Here, we replace the undesirable MaxValue random generator with a smaller constant.
      case RandomFaker(ConstantFaker(-1L), ConstantFaker(Int.MaxValue)) => ConstantFaker(10L)
      case other                                                        => other
    })
  }
}

/** A faker that uses DataFaker to generate a string. */
private[this] case class StringExpressionFaker(expressionFn: AvroFaker[String]) extends AvroFaker[String] {
  private var faker: Option[Faker] = None
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

  def apply(setup: SetupContext, key: String): AvroFaker[_] = {
    val faker = fake(setup.args, key)
    if (setup.asJava) new AvroFaker[Utf8] {
      override def apply(ctx: FakerContext): Utf8 = new Utf8(faker(ctx))
    }
    else faker
  }
}

/** A faker that generates byte arrays. */
private[this] case class BytesFaker(lengthFn: AvroFaker[Long]) extends AvroFaker[Seq[Byte]] {
  def apply(ctx: FakerContext): Seq[Byte] = ctx.rnd.nextBytes(lengthFn(ctx).toInt).toSeq
}

private[this] object BytesFaker {
  def apply(setup: SetupContext): AvroFaker[_] = {
    setup.schema.getType match {
      case Schema.Type.FIXED =>
        val faker = BytesFaker(lengthFn = ConstantFaker(setup.schema.getFixedSize))
        if (setup.asJava) new AvroFaker[Fixed] {
          override def apply(ctx: FakerContext): Fixed = new Fixed(setup.schema, faker(ctx).toArray)
        }
        else faker
      case _ =>
        val faker = BytesFaker(lengthFn = NumberFaker[Long]((-1, Int.MaxValue), setup.args, ArgLength) match {
          // Here, we replace the undesirable MaxValue random generator with a smaller constant.
          case RandomFaker(ConstantFaker(-1L), ConstantFaker(Int.MaxValue)) => RandomFaker(16L, 33L)
          case other                                                        => other
        })
        if (setup.asJava) new AvroFaker[ByteBuffer]() {
          override def apply(ctx: FakerContext): ByteBuffer = ByteBuffer.wrap(faker(ctx).toArray)
        }
        else faker
    }
  }
}

private[this] object NumberFaker {

  /** Tests a numeric value and ensures that it is between the bounds (inclusive). */
  def forceBounds[T](value: T, lower: T, upper: T)(implicit num: Numeric[T]): T = {
    import num._
    value min upper max lower
  }

  /** For the numbers that we handle, convert them to a numeric type that we wish to use during generation. Note that we
    * only use Doubles and Longs, which are set to the bounds or precision of Int and Float at the last minute, but we
    * handle Integers just in case they are explicitly set.
    */
  private def toNumOption[T](value: Any)(implicit num: Numeric[T]): Option[T] = {
    val pass1: Option[Any] = Option((num, value)).collect {
      case (_, false)                         => num.zero
      case (_, true)                          => num.one
      case (_, "false")                       => num.zero
      case (_, "true")                        => num.one
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

  def apply[T](bounds: (T, T), args: Map[String, Any], key: String, dflt: Any = Map(ArgFaker -> StrategyRandom))(
      implicit num: Numeric[T]
  ): AvroFaker[T] = {

    // Get the key from the args or defaults
    val value: Option[Any] = args.get(key)

    // Shortcut defaults for the bounds
    if (key == ArgMin && value.isEmpty) return ConstantFaker(bounds._1)
    if (key == ArgMax && value.isEmpty) return ConstantFaker(bounds._2)

    // Get any explicitly specified strategy.
    val explicitFn: Option[AvroFaker[T]] = value collect {
      case StrategyRandom   => RandomFaker(bounds, args)
      case StrategyGauss    => DoubleGaussFaker(bounds, args)
      case StrategySequence => SequenceFaker[T](bounds, args)
      case StrategyValue    => BoundedFaker(bounds, args)
      case StrategyOneOf | StrategySumOf | StrategyProductOf | StrategyMinOf | StrategyMaxOf | StrategyMeanOf =>
        NumberFaker(bounds, args, args(key).toString, dflt)(num) match {
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
      case StrategyWeights => WeightsFaker(bounds, args)
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
      if (implicitFn.nonEmpty) return NumberFaker(bounds, args ++ implicitFn.get, key, dflt)(num)
    }

    // Otherwise, interpret the strategy depending on its type
    value match {
      case Some(m: Map[_, _]) =>
        // A map, or object, adds attributes to the strategy that can complete how it is interpreted.
        // TODO: This is pretty complicated, removing some attributes from the existing faker strategy in
        // order to use the attributes in the extra object, when what we *really* want is to prioritize the
        // attributes in m to implicitly choose the strategy, then try the combined attributes.
        val keysToRemove: immutable.Iterable[String] = NumberArgToImplicitStrategy
          .groupMap(_._2)(_._1)
          .getOrElse(args.getOrElse(ArgFaker, "").toString, Seq.empty)
          .toSeq :+ ArgFaker :+ key
        NumberFaker[T](bounds, args.removedAll(keysToRemove) ++ m.asInstanceOf[Map[String, Any]], ArgFaker, dflt)
      case Some(xs: Iterable[_]) =>
        // An array is, by default, a list of strategies that we randomly pick one from.
        RandomOneOfFaker(xs.map(v => NumberFaker[T](bounds, args ++ Map(key -> v), key, dflt)).toSeq)
      case None =>
        // We fall back to the default value.
        NumberFaker[T](bounds, args ++ Map(key -> dflt), key, 0)
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
  */
case class IntFaker(args: Map[String, Any]) extends AvroFaker[Int] {
  private val fn = NumberFaker[Long]((Int.MinValue.toLong, Int.MaxValue.toLong), args, ArgFaker)
  def apply(ctx: FakerContext): Int = fn(ctx).toInt
}

/** Generates LONG values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class LongFaker(args: Map[String, Any]) extends AvroFaker[Long] {
  private val fn = NumberFaker[Long]((Long.MinValue, Long.MaxValue), args, ArgFaker)
  def apply(ctx: FakerContext): Long = fn(ctx)
}

/** Generates FLOAT values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class FloatFaker(args: Map[String, Any]) extends AvroFaker[Float] {
  private val fn =
    NumberFaker[Double]((Float.NegativeInfinity.toDouble, Float.PositiveInfinity.toDouble), args, ArgFaker)
  def apply(ctx: FakerContext): Float = fn(ctx).toFloat
}

/** Generates DOUBLE values with a specific strategy given by the configuration.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class DoubleFaker(args: Map[String, Any]) extends AvroFaker[Double] {
  private val fn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgFaker)
  def apply(ctx: FakerContext): Double = fn(ctx)
}

/** A faker that returns a value bounded by a minimum or a maximum. */
private[this] case class BoundedFaker[T](minFn: AvroFaker[T], maxFn: AvroFaker[T], fn: AvroFaker[T])(implicit
    num: Numeric[T]
) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = NumberFaker.forceBounds(fn(ctx), minFn(ctx), maxFn(ctx))
}

private[this] object BoundedFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): BoundedFaker[T] =
    BoundedFaker(
      minFn = NumberFaker(bounds, args, ArgMin)(num),
      maxFn = NumberFaker(bounds, args, ArgMax)(num),
      fn = NumberFaker(bounds, args, StrategyValue)(num)
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
    extends AvroFaker[T] {
  import num._
  import NumberFaker._
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

private[this] object RandomFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): RandomFaker[T] = {
    val defaultBounds: (T, T) = num match {
      case _: Fractional[T] => (num.zero, num.one)
      case _                => bounds
    }
    RandomFaker(
      minFn = NumberFaker(defaultBounds, args, ArgMin),
      maxFn = NumberFaker(defaultBounds, args, ArgMax)
    )
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
    extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = {
    val min = minFn(ctx)
    val max = maxFn(ctx)
    val mean = meanFn(ctx)
    val stddev = stddevFn(ctx)
    val next = LazyList.continually(ctx.rnd.nextGaussian() * stddev + mean).dropWhile(_ < min).dropWhile(_ >= max).head
    NumberFaker.toNum(next)(num)
  }
}

private[this] object DoubleGaussFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): DoubleGaussFaker[T] = {

    /** Fractional numbers have a standard deviation of 100 instead of 1 so we see a wider range of values. */
    val defaultStddev: Double = num match {
      case _: Fractional[T] => 1.0
      case _                => 100.0
    }

    DoubleGaussFaker[T](
      minFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMin, bounds._1),
      maxFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMax, bounds._2),
      meanFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgMean, 0),
      stddevFn = NumberFaker[Double]((Double.NegativeInfinity, Double.PositiveInfinity), args, ArgStddev, defaultStddev)
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
    extends AvroFaker[T] {
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

private[this] object SequenceFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): SequenceFaker[T] = {
    SequenceFaker(
      minFn = NumberFaker[T](bounds, args, ArgMin),
      maxFn = NumberFaker[T](bounds, args, ArgMax),
      stepFn = NumberFaker[T](bounds, args, ArgStep, 1),
      startFn = if (args.contains(ArgStart)) Some(NumberFaker[T](bounds, args, ArgStart)(num)) else None,
      overridesMin = args.contains(ArgMin)
    )
  }
}

private[this] case class WeightsConstantFaker[T](max: Int, weights: Seq[Double])(implicit num: Numeric[T])
    extends AvroFaker[T] {

  // Count the number of elements that are weighted (n) and unweighted (m) and the sum of their weights
  private[this] val n = (weights.size - weights.reverse.takeWhile(_ == 1.0).size) min max
  private[this] val nSum = weights.take(n).sum
  private[this] val m = (max - n) max 0
  private[this] val mSum = m // 1.0 each

  def apply(ctx: FakerContext): T = {
    // We choose either the N set or the M set with the correct probability
    val index: Int = if (mSum == 0 || nSum != 0 && ctx.rnd.nextDouble() < nSum / (nSum + mSum)) {
      // If there's only one number in the weighted set, then return it without consuming a random
      if (n <= 1) 0
      else {
        // Otherwise find a double value somewhere in the weighted sum, and pick where it lands
        val countdown = LazyList.from(weights).scanLeft(ctx.rnd.nextDouble() * nSum) { _ - _ }
        countdown.tail.zipWithIndex.find(_._1 < 0).map(_._2).getOrElse(n - 1)
      }
    } else {
      // Choosing from the M set if there's more than one, just give all the elements an equal chance
      if (m <= 1) n
      else n + ctx.rnd.nextInt(m)
    }

    NumberFaker.toNum[T](index)
  }
}

private[this] case class WeightFaker[T](maxFn: AvroFaker[Int], weightFns: Seq[AvroFaker[Double]])(implicit
    num: Numeric[T]
) extends AvroFaker[T] {
  def apply(ctx: FakerContext): T = WeightsConstantFaker[T](maxFn(ctx), weightFns.map(_(ctx))).apply(ctx)
}

private[this] object WeightsFaker {
  def apply[T](bounds: (T, T), args: Map[String, Any])(implicit num: Numeric[T]): AvroFaker[T] = {
    import num._
    val maxFn: AvroFaker[Int] = NumberFaker((0, (num.fromInt(Int.MaxValue) min bounds._2).toInt), args, ArgMax)
    val weightFns: Seq[AvroFaker[Double]] =
      NumberFaker[Double]((0d, Double.MaxValue), args, StrategyWeights, 1) match {
        case RandomOneOfFaker(fns) => fns
        case other                 => Seq(other)
      }
    maxFn match {
      case ConstantFaker(max) if weightFns.forall(_.isInstanceOf[ConstantFaker[_]]) =>
        WeightsConstantFaker[T](max = max, weights = weightFns.collect[Double] { case ConstantFaker(weight) => weight })
      case _ => WeightFaker(maxFn = maxFn, weightFns = weightFns)
    }
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

/** Generates Boolean values with a specific strategy given by the schema properties.
  *
  * @param args
  *   The annotations that have been assigned to the schema, or to the faker strategy.
  */
case class BooleanFaker(args: Map[String, Any]) extends AvroFaker[Boolean] {
  private val fn = NumberFaker[Long]((0, 2), args, ArgFaker)
  def apply(ctx: FakerContext): Boolean = fn(ctx) != 0
}

/** A NULL schema generates only null. */
case object NullFaker extends AvroFaker[Void] {
  def apply(ctx: FakerContext): Void = null
}
