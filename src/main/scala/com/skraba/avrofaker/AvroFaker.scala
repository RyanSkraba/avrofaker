package com.skraba.avrofaker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{
  ArrayNode,
  DoubleNode,
  FloatNode,
  IntNode,
  JsonNodeFactory,
  LongNode,
  NullNode,
  ObjectNode,
  TextNode
}
import com.skraba.avrofaker.AvroFaker._
import net.datafaker.Faker
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed}
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecordBuilder, IndexedRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.Utf8

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap
import scala.util.{Random, Using}

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
case class SetupContext(schema: Schema, parentArgs: Map[String, Any], asJava: Boolean, rnd: Random) {
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

/** Each AvroFaker creates a type of datum, depending on the schema. */
trait AvroFaker[T] extends (FakerContext => T) {
  def apply(ctx: FakerContext): T
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
        if (setup.asJava) (ctx: FakerContext) => new EnumSymbol(setup.schema, faker(ctx))
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

  def apply(schema: Schema, rnd: Random = new Random()): AvroFaker[_] = apply(
    SetupContext(schema, Map.empty, asJava = true, rnd)
  )

  /** Creates a fake datum as an Avro-serialized JSON. */
  def asAvroJson(schema: Schema, rnd: Random = new Random()): AvroFaker[String] = {
    val faker = apply(SetupContext(schema, Map.empty, asJava = true, rnd))
    ctx => {
      Using.resource(new ByteArrayOutputStream) { baos =>
        val datum = faker(ctx)
        val encoder = EncoderFactory.get.jsonEncoder(schema, baos, false)
        val w = new GenericDatumWriter[Any](schema, GenericData.get)
        w.write(datum, encoder)
        encoder.flush()
        new String(baos.toByteArray, StandardCharsets.UTF_8)
      }
    }
  }

  /** Creates a fake datum as JSON, using slightly different rules. This can't be deserialized as JSON but doesn't
    * insert extra types for unions.
    */
  def asSlimJson(schema: Schema, rnd: Random = new Random()): AvroFaker[String] = {
    val faker = apply(SetupContext(schema, Map.empty, asJava = false, rnd))
    def toSlimJson(in: Any): JsonNode = in match {
      case m: Map[_, _] =>
        new ObjectNode(
          JsonNodeFactory.instance,
          m.collect { case (key, value) => key.toString -> toSlimJson(value) }.asJava
        )
      case record: IndexedRecord =>
        new ObjectNode(
          JsonNodeFactory.instance,
          record.getSchema.getFields.asScala
            .collect { case field: Field => field.name() -> toSlimJson(record.get(field.pos())) }
            .toMap
            .asJava
        )
      case a: Seq[_]                       => new ArrayNode(JsonNodeFactory.instance, a.map(toSlimJson).asJava)
      case i: Int                          => new IntNode(i)
      case l: Long                         => new LongNode(l)
      case f: Float                        => new FloatNode(f)
      case d: Double                       => new DoubleNode(d)
      case s: String                       => new TextNode(s)
      case other if Option(other).nonEmpty => new TextNode(in.toString)
      case _                               => NullNode.instance
    }

    ctx => toSlimJson(faker(ctx)).toString
  }
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
      lengthFn = NumberFaker.longOrElse(setup.args, ArgLength, NumberFaker.random(3L, 6L)),
      fn = AvroFaker(setup.copy(schema = setup.schema.getElementType, parentArgs = setup.args))
    )
    if (setup.asJava) (ctx: FakerContext) => faker(ctx).asJava
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
      lengthFn = NumberFaker.longOrElse(setup.args, ArgLength, NumberFaker.random(3L, 6L)),
      keyFn = StringFaker(setup, ArgKey),
      fn = AvroFaker(setup.copy(schema = setup.schema.getValueType, parentArgs = setup.args))
    )
    if (setup.asJava) (ctx: FakerContext) => faker(ctx).asJava
    else faker
  }
}

/** A faker that generates a random string. */
private[this] case class StringRandomFaker(lengthFn: AvroFaker[Long]) extends AvroFaker[String] {
  def apply(ctx: FakerContext): String = ctx.rnd.alphanumeric.take(lengthFn(ctx).toInt).mkString
}

private[this] object StringRandomFaker {
  def apply(args: Map[String, Any]): StringRandomFaker = {
    StringRandomFaker(lengthFn = NumberFaker.longOrElse(args, ArgLength, ConstantFaker(10L)))
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
    if (setup.asJava) (ctx: FakerContext) => new Utf8(faker(ctx))
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
        if (setup.asJava) (ctx: FakerContext) => new Fixed(setup.schema, faker(ctx).toArray)
        else faker
      case _ =>
        val faker = BytesFaker(lengthFn = NumberFaker.longOrElse(setup.args, ArgLength, NumberFaker.random(16L, 33L)))
        if (setup.asJava) new AvroFaker[ByteBuffer]() {
          override def apply(ctx: FakerContext): ByteBuffer = ByteBuffer.wrap(faker(ctx).toArray)
        }
        else faker
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
