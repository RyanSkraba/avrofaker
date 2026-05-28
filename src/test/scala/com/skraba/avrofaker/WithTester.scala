package com.skraba.avrofaker

import org.apache.avro.Schema
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag
import scala.util.{Random, Try}

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
    def adaptSchemaWithType(schema: String): String = schema match {
      case _ if schema.contains("<TYPE>")                     => schema.replace("<TYPE>", sType.toString.toLowerCase())
      case _ if schema == s"""{"type": "${sType.getName}"}""" => schema
      case _ if schema.startsWith("{") => s"""{"type": "${sType.getName}", ${schema.substring(1)}"""
      case _                           => schema
    }

    /** @param schema
      *   A schema to modify with the given properties
      * @param props
      *   Pairs of property keys and values to apply to the schema
      * @return
      *   The instance of the schema passed in, with the properties applied
      */
    def applyProps(schema: Schema, props: (String, Any)*): Schema = {
      for ((key, value) <- props)
        schema.addProp(key, value)
      schema
    }

    /** Create an AvroFaker from the given schema with the given properties.
      *
      * @param schema
      *   The base schema to use
      * @param props
      *   Pairs of property keys and values to apply to the schema
      * @return
      *   A LazyList stream of fake values.
      */
    def generate(schema: Schema, props: (String, Any)*): LazyList[T] = {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(applyProps(schema, props: _*))
      LazyList.continually(gen(ctx)).flatMap {
        case good: T => Some(good)
        case bad     =>
          // This will fail here
          bad shouldBe a[T]
          None
      }
    }

    def generate(schema: String, props: (String, Any)*): LazyList[T] =
      generate(new Schema.Parser().parse(schema), props: _*)

    def generate(props: (String, Any)*): LazyList[T] = generate(Schema.create(sType), props: _*)

    case class ItTestExpected(description: String, schema: String, fn: Any => Any) {

      /** Produces the test to be executed (an `it` word). */
      def execute(expected: Seq[_]): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(schema).take(expected.size)

        it(s"$description: $schema") {
          values.map(fn) shouldBe expected.map(fn)
        }
      }
    }

    class Dist(val xs: (_, Long)*) {

      /** Produces the test to be executed (an `it` word). */
      def execute(description: String, schema: String, fn: Any => Any): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(schema).take(xs.map(_._2).sum.toInt)

        it(s"$description: $schema") {
          val actualDistribution = values.map(fn).groupBy(identity).view.mapValues(_.size).toMap
          withClue(actualDistribution.mkString("Found: Map(", ",", ")")) {
            actualDistribution shouldBe xs.collect { case (k, v) => fn(k) -> v }.toMap
          }
        }
      }
    }

    class Applies(description: String, fn: Any => Any = valueCompareFn) {
      def apply(schema: String, dist: Dist): Unit =
        dist.execute(description, adaptSchemaWithType(schema), fn = fn)

      def apply(schema: String, expected: Seq[_]): Unit =
        ItTestExpected(description, adaptSchemaWithType(schema), fn = fn).execute(expected)

      def apply(cases: (String, Seq[_])*): Unit = {
        for ((schema, expected) <- cases) apply(schema, expected)
      }
    }

    def apply(description: String): Applies = {
      new Applies(description)
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

    private[this] case class FractionalIt(description: String, schema: String) {

      /** Produces the test to be executed (an `it` word). */
      def execute(expected: Seq[_]): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(schema).take(expected.size)

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
      }

      private[this] def compare(f1: Float, f2: Float): Assertion = f1 shouldBe (f2 +- 1e-7f)

      private[this] def compare(d1: Double, d2: Double): Assertion = d1 shouldBe (d2 +- 1e-14d)
    }

    class NumericApplies(description: String) extends Applies(description, fn = valueCompareFn) {
      override def apply(schema: String, expected: Seq[_]): Unit = {
        if (isIntegral)
          super.apply(adaptSchemaWithType(schema), expected)
        else
          FractionalIt(description, adaptSchemaWithType(schema)).execute(expected)
      }
    }

    override def apply(description: String): NumericApplies = {
      new NumericApplies(description)
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
