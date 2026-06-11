package com.skraba.avrofaker

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, IndexedRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.reflect.ClassTag
import scala.util.{Random, Try, Using}

trait WithTester extends AnyFunSpecLike with Matchers {

  /** This tester helps run suites by generating data.
    *
    * @param sType
    *   The Avro numeric Schema.Type
    * @param valueCompareFn
    *   A function to apply to every generated value and expected value before doing a comparison.
    * @tparam T
    *   The type of datum that AvroFaker should be generating
    */
  class Tester[T](val sType: Schema.Type, val valueCompareFn: Any => Any)(implicit ct: ClassTag[T]) {

    /** Use the given GenericData model to serialize the datum according to the schema. */
    def toBytes(model: GenericData, schema: Schema, datum: T): Array[Byte] =
      Using(new ByteArrayOutputStream())(baos => {
        val encoder = EncoderFactory.get.binaryEncoder(baos, null)
        val w = new GenericDatumWriter[T](schema, model)
        w.write(datum, encoder)
        encoder.flush()
        baos.toByteArray
      }).get

    /** Use the given GenericData to deserialize a datum from the bytes according to the schema. */
    def fromBytes(model: GenericData, writer: Schema, reader: Schema, serialized: Array[Byte]): T =
      Using(new ByteArrayInputStream(serialized))(bais => {
        val decoder = DecoderFactory.get.binaryDecoder(bais, null)
        val r = new GenericDatumReader[T](writer, reader, model)
        r.read(null.asInstanceOf[T], decoder)
      }).get

    /** Create an AvroFaker from the given schema with the given properties.
      *
      * @param schema
      *   The base schema to use
      * @param props
      *   Pairs of property keys and values to apply to the schema
      * @return
      *   A LazyList stream of fake values.
      */
    def generate(schema: Schema): LazyList[T] = {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(SetupContext(schema, Map.empty, asJava = false, new Random(0)))
      LazyList.continually(gen(ctx)).map {
        case null => null.asInstanceOf[T]
        case x =>
          x shouldBe a[T]
          x.asInstanceOf[T]
      }
    }

    /** Generate a different seed for each round trip. */
    val RoundTripSeed: Long = new Random().nextLong()

    /** The number of generated values to test in a round trip. */
    val DefaultRoundTrips: Int = 1000

    trait TesterCase {
      val description: String
      val schema: String
      val avroSchema: Schema = new Schema.Parser().parse(adaptSchemaWithType(schema))

      def execute(): Unit

      /** Given the schema under test, injects the Avro type from this helper.
        *
        * This will either
        *   - Replace any occurrences of &lt;TYPE&gt; by the appropriate Avro schema type, or
        *   - If it looks like a JSON object without a "type": attribute, it will rewrite to add this attribute,
        *   - Leave it alone.
        *
        * @param schema
        *   A string
        * @return
        *   The schema with the helper type inserted.
        */
      private[this] def adaptSchemaWithType(schema: String): String = schema match {
        case _ if schema.contains("<TYPE>") => schema.replace("<TYPE>", sType.toString.toLowerCase())
        case _ if schema == s"""{"type": "${sType.getName}"}""" => schema
        case _ if schema.startsWith("{") => s"""{"type": "${sType.getName}", ${schema.substring(1)}"""
        case _                           => schema
      }

      def roundTrip(count: Int): Unit = {
        if (count > 0) {
          val ctx = FakerContext(new Random(RoundTripSeed))
          val gen = AvroFaker(SetupContext(avroSchema, Map.empty, asJava = true, new Random(RoundTripSeed)))
          it(s"$description: $schema (round-trip #${RoundTripSeed})") {
            LazyList.continually(gen(ctx)).take(count).foreach { datum =>
              val bytes = toBytes(GenericData.get, avroSchema, datum.asInstanceOf[T])
              val copy: T = fromBytes(GenericData.get, avroSchema, avroSchema, bytes)
              datum shouldBe copy
            }
          }
        }
      }

    }

    class ReturnedValuesTesterCase(
        override val description: String,
        override val schema: String,
        fn: Any => Any,
        expected: Seq[_],
        roundTrip: Int
    ) extends TesterCase {

      /** Produces the test to be executed (an `it` word). */
      def execute(): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(avroSchema).take(expected.size)

        it(s"$description: $schema") {
          values.map(fn) shouldBe expected.map(fn)
        }

        // Potentially add a round trip test case
        roundTrip(roundTrip)
      }
    }

    class Dist(val xs: (_, Int)*) {
      def this(ds0: Int, ds: Int*) = this((ds0 +: ds).zipWithIndex.map(_.swap): _*)
    }

    class DistributionTesterCase(description: String, schema: String, fn: Any => Any, expected: Dist)
        extends ReturnedValuesTesterCase(description, schema, fn, Seq.empty, -1) {

      /** Produces the test to be executed (an `it` word). */
      override def execute(): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(avroSchema).take(expected.xs.map(_._2).sum)

        it(s"$description: $schema") {
          val actualDistribution = values.map(fn).groupBy(identity).view.mapValues(_.size).toMap

          val clue = actualDistribution match {
            case actual if (0 until actual.size).forall(actual.keySet) =>
              (0 until actual.size).map(actual).mkString("Dist(", ",", ")")
            case actual =>
              actual.toSeq
                .sortBy(any => (any._1.toString.toLongOption.getOrElse(Long.MaxValue), any.toString))
                .map { case (k, v) => s"$k->$v" }
                .mkString("Dist(", ",", ")")
          }

          withClue(clue) {
            actualDistribution shouldBe expected.xs.collect { case (k, v) => fn(k) -> v }.toMap
          }
        }
      }
    }

    class Applies(description: String, fn: Any => Any = valueCompareFn, roundTrip: Int) {

      def apply(schema: String, expected: Seq[_]): Unit =
        new ReturnedValuesTesterCase(description, schema, fn = fn, expected, roundTrip = roundTrip)
          .execute()

      def apply(cases: (String, Seq[_])*): Unit = {
        for ((schema, expected) <- cases) apply(schema, expected)
      }

      def apply(schema: String, expected: Dist): Unit =
        new DistributionTesterCase(description, schema, fn = fn, expected).execute()

      def apply(case0: (String, Dist), cases: (String, Dist)*): Unit = {
        apply(case0._1, case0._2)
        for ((schema, expected) <- cases) apply(schema, expected)
      }
    }

    def apply(description: String, roundTrip: Int = DefaultRoundTrips): Applies = {
      new Applies(description, roundTrip = roundTrip)
    }
  }

  /** This tester helps run suites on all four types of numeric values: INT, LONG, FLOAT and DOUBLE.
    *
    * @param sType
    *   The Avro numeric Schema.Type
    * @tparam T
    *   The type of datum that AvroFaker should be generating
    */
  class NumericTester[T](override val sType: Schema.Type)(implicit ct: ClassTag[T], num: Numeric[T])
      extends Tester[T](
        sType,
        num match {
          case _: Integral[T] => Tester.toLong
          case _              => Tester.toDouble
        }
      )(ct) {

    /** True if we are matching a whole number type, false for floating point. */
    val isIntegral: Boolean = num match {
      case _: Integral[T] => true
      case _              => false
    }

    private[this] class ValueByValueFpTesterCase(description: String, schema: String, expected: Seq[_], roundTrip: Int)
        extends ReturnedValuesTesterCase(description, schema, identity, expected, roundTrip = roundTrip) {

      /** Produces the test to be executed (an `it` word). */
      override def execute(): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(avroSchema).take(expected.size)

        it(s"$description: $schema") {
          withClue(values.mkString("Found: Seq(", ",", ")")) {
            // Values need to be compared one at a time.
            values.zip(expected).foreach {
              case (f1: Float, f2: Float) if f1 == f2 || f1.isNaN && f2.isNaN   => ()
              case (d1: Double, d2: Double) if d1 == d2 || d1.isNaN && d2.isNaN => ()
              case (f1: Float, f2: Any)                                         => compare(f1, f2.toString.toFloat)
              case (d1: Double, d2: Any)                                        => compare(d1, d2.toString.toDouble)
            }
          }
        }

        roundTrip(roundTrip)
      }

      private[this] def compare(f1: Float, f2: Float): Assertion = f1 shouldBe (f2 +- 1e-7f)

      private[this] def compare(d1: Double, d2: Double): Assertion = d1 shouldBe (d2 +- 1e-14d)
    }

    class NumericApplies(description: String, roundTrip: Int)
        extends Applies(description, fn = valueCompareFn, roundTrip) {
      override def apply(schema: String, expected: Seq[_]): Unit = {
        if (isIntegral)
          super.apply(schema, expected)
        else
          new ValueByValueFpTesterCase(description, schema, expected, roundTrip).execute()
      }
    }

    override def apply(description: String, roundTrip: Int = DefaultRoundTrips): NumericApplies = {
      new NumericApplies(description, roundTrip)
    }
  }

  object Tester {
    val Int = new NumericTester[Int](Schema.Type.INT)
    val Long = new NumericTester[Long](Schema.Type.LONG)
    val Float = new NumericTester[Float](Schema.Type.FLOAT)
    val Double = new NumericTester[Double](Schema.Type.DOUBLE)
    val String = new Tester[String](Schema.Type.STRING, identity)
    val Array = new Tester[Array[_]](
      Schema.Type.ARRAY,
      {
        case xs: Array[_] => xs.toSeq
        case other        => other
      }
    )
    val Map = new Tester[Map[String, _]](Schema.Type.MAP, identity)
    val Bytes = new Tester[Seq[Byte]](
      Schema.Type.BYTES,
      {
        case xs: Iterable[_] => new String(java.util.Base64.getEncoder.encode(xs.map(toByte).toArray))
        case other           => other
      }
    )
    val Fixed = new Tester[Seq[Byte]](
      Schema.Type.FIXED,
      {
        case xs: Iterable[_] => new String(java.util.Base64.getEncoder.encode(xs.map(toByte).toArray))
        case other           => other
      }
    )
    val Enum = new Tester[String](Schema.Type.ENUM, identity)
    val Union = new Tester[Any](Schema.Type.UNION, identity)
    val Boolean = new Tester[Boolean](Schema.Type.BOOLEAN, identity)
    val Null = new Tester[Any](Schema.Type.NULL, identity)
    val Record = new Tester[IndexedRecord](Schema.Type.RECORD, _.toString)

    def toByte(in: Any): Byte = in match {
      case b: Byte   => b
      case d: Double => d.toByte
      case i: Int    => i.toByte
      case l: Long   => l.toByte
      case other     => Try(other.toString.toByte).orElse(Try(other.toString.toDouble.toByte)).get
    }

    def toLong(in: Any): Long = in match {
      case d: Double => d.toLong
      case i: Int    => i
      case l: Long   => l
      case other     => Try(other.toString.toLong).orElse(Try(other.toString.toDouble.toLong)).get
    }

    def toDouble(in: Any): Double = in match {
      case i: Int    => i.toDouble
      case l: Long   => l.toDouble
      case f: Float  => f.toDouble
      case d: Double => d
      case other     => other.toString.toDouble
    }
  }
}
