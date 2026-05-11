package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag
import scala.util.{Random, Try}

class AvroFakerSpec extends AnyFunSpecLike with Matchers {

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

  val IntSequence: Schema = applyProps(Schema.create(Schema.Type.INT), ArgStep -> 1)

  /** This tester helps run suites by generating data.
    *
    * @param sType
    *   The Avro numeric Schema.Type
    * @tparam T
    *   The type of datum that AvroFaker should be generating
    */
  class Tester[T](val sType: Schema.Type)(implicit ct: ClassTag[T]) {

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
  }

  /** This tester helps run suites on all four types of numeric values: INT, LONG, FLOAT and DOUBLE.
    *
    * @param sType
    *   The Avro numeric Schema.Type
    * @tparam T
    *   The type of datum that AvroFaker should be generating
    */
  class NumericTester[T](override val sType: Schema.Type)(implicit ct: ClassTag[T], num: Numeric[T])
      extends Tester[T](sType)(ct) {

    /** True if we are matching a whole number type, false for floating point. */
    val isIntegral: Boolean = num match {
      case _: Integral[T] => true
      case _              => false
    }

    private[this] case class IntegralIt(description: String, schema: String) {

      /** Produces the test to be executed (an `it` word). */
      def execute(expected: Seq[_]): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(schema).take(expected.size)

        it(s"$description: $schema") {
          // Truncate any values that are floating point.
          val expectedAsLong = expected.map {
            case d: Double => d.toLong
            case i: Int    => i
            case l: Long   => l
            case other     => Try(other.toString.toLong).orElse(Try(other.toString.toDouble.toLong)).get
          }
          values shouldBe expectedAsLong
        }
      }
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
    }

    private[this] def compare(f1: Float, f2: Float): Assertion = f1 shouldBe (f2 +- 1e-7f)

    private[this] def compare(d1: Double, d2: Double): Assertion = d1 shouldBe (d2 +- 1e-14d)

    class Cases(description: String) {
      def apply(cases: (String, Seq[_])*): Unit = {
        for ((schema, expected) <- cases) {
          if (isIntegral)
            IntegralIt(description, schema.replace("<TYPE>", sType.toString.toLowerCase())).execute(expected)
          else
            FractionalIt(description, schema.replace("<TYPE>", sType.toString.toLowerCase())).execute(expected)
        }
      }
    }

    def apply(description: String): Cases = {
      new Cases(description)
    }
  }

  object Tester {
    val Int = new NumericTester[Int](Schema.Type.INT)
    val Long = new NumericTester[Long](Schema.Type.LONG)
    val Float = new NumericTester[Float](Schema.Type.FLOAT)
    val Double = new NumericTester[Double](Schema.Type.DOUBLE)
    val String = new Tester[String](Schema.Type.STRING)
  }

  def randomStrategy[T](helper: NumericTester[T]): Unit = {
    describe(s"Generate ${helper.sType} with the random strategy") {
      val aNumber = helper.apply _

      /** Expected results over the default random range */
      val random = helper.sType match {
        case Schema.Type.INT =>
          Seq(-1630935619, -1483802595, -864264928)
        case Schema.Type.LONG =>
          Seq(-4962768465676381896L, 4437113781045784766L, -6688467811848818630L)
        case _ =>
          Seq(0.730967787376657, 0.24053641567148587, 0.6374174253501083)
      }

      /** Expected results over the default positive range */
      val withMinimum = helper.sType match {
        case Schema.Type.INT =>
          Seq(711764125, 1302116448, 663681053)
        case Schema.Type.LONG =>
          Seq(1340999404015745395L, 4053417136674644310L, 8448822068277548669L)
        case _ =>
          Seq(0.8654838936883285, 0.6202682078357429, 0.8187087126750541)
      }

      /** Expected results over [0, 256) */
      val byteSize = helper.sType match {
        case Schema.Type.INT | Schema.Type.LONG =>
          Seq(187, 212, 61, 155, 163, 79, 140, 29, 152, 200)
        case _ =>
          Seq(187.1277535684242, 61.57732241190038, 163.1788608896277, 140.9118733101143, 152.97159111608366,
            85.30391026602234, 98.60843129362394, 252.1194342911511, 225.07072457535492, 240.95978994742129)
      }

      aNumber("should use the random strategy on unannotated schemas")(
        """"<TYPE>"""" -> random,
        """{"type": "<TYPE>"}""" -> random
      )

      aNumber("should generate a random Int when the random strategy is explicitly set")(
        """{"type": "<TYPE>", "faker": "random"}""" -> random,
        """{"type": "<TYPE>", "faker": {"faker": "random"}}""" -> random
      )

      aNumber("should generate a random Int when the random strategy is unset")(
        """{"type": "<TYPE>", "faker": {}}""" -> random
      )

      if (helper.isIntegral) {
        aNumber("should allow configuring the random strategy with a minimum")(
          """{"type": "<TYPE>", "min": 0, "faker": "random"}""" -> withMinimum,
          """{"type": "<TYPE>", "faker": {"min": 0}}""" -> withMinimum
        )

        aNumber("should implicitly configure the random strategy from the schema annotations")(
          """{"type": "<TYPE>", "min": 0}""" -> withMinimum
        )

        aNumber("should override arguments from the schema annotations")(
          """{"type": "<TYPE>", "min": 100, "faker": {"min": 0}}""" -> withMinimum
        )
      } else {
        aNumber("should allow configuring the random strategy with a minimum")(
          """{"type": "<TYPE>", "min": 0.5, "faker": "random"}""" -> withMinimum,
          """{"type": "<TYPE>", "faker": {"min": 0.5}}""" -> withMinimum
        )

        aNumber("should implicitly configure the random strategy from the schema annotations")(
          """{"type": "<TYPE>", "min": 0.5}""" -> withMinimum
        )

        aNumber("should override arguments from the schema annotations")(
          """{"type": "<TYPE>", "min": -0.5, "faker": {"min": 0.5}}""" -> withMinimum
        )
      }

      aNumber("should be configurable by the min and the max")(
        """{"type": "<TYPE>", "min": 0, "max": 256}""" -> byteSize,
        """{"type": "<TYPE>", "faker": {"min": 0, "max": 256}}""" -> byteSize,
        """{"type": "<TYPE>", "min": 0, "faker": {"max": 256}}""" -> byteSize,
        """{"type": "<TYPE>", "min": 100, "faker": {"min": 0, "max": 256}}""" -> byteSize
      )
    }
  }
  randomStrategy(Tester.Int)
  randomStrategy(Tester.Long)
  randomStrategy(Tester.Float)
  randomStrategy(Tester.Double)

  def gaussStrategy[T](helper: NumericTester[T]): Unit = {
    describe(s"Generate ${helper.sType} with the gauss strategy") {
      val aNumber = helper.apply _

      val stddev = if (helper.isIntegral) "100" else "1"

      /** Expected results over the default gauss range */
      val dflt = helper.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(80, -90, 208, 76, 98, -168, -2, 11, -39, -64)
        case _ =>
          Seq(0.8025330637390305, -0.9015460884175122, 2.080920790428163, 0.7637707684364894, 0.9845745328825128,
            -1.6834122587673428, -0.027290262907887285, 0.11524570286202315, -0.39016704137993785, -0.6433888131264491)
      }

      val gauss100_10 = helper.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(108, 90, 120, 107, 109, 83, 99, 101, 96, 93)
        case _ =>
          Seq(108.02533063739031, 90.98453911582487, 120.80920790428164, 107.6377076843649, 109.84574532882513,
            83.16587741232658, 99.72709737092113, 101.15245702862023, 96.09832958620062, 93.5661118687355)
      }

      val gauss100_10NonNeg = helper.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(108, 120, 107, 109, 101, 100, 105, 102, 114, 102)
        case _ =>
          Seq(108.02533063739031, 120.80920790428164, 107.6377076843649, 109.84574532882513, 101.15245702862023,
            100.52460907198835, 105.21342076929889, 102.60718194028357, 114.03147381720936, 102.71130617070202)
      }

      aNumber(
        "should generate between a bell curve of numbers centered around 0, with 95% of the values between -200 and 200"
      )(
        // These are all equivalent
        s"""{"type": "<TYPE>", "faker": "gauss"}""" -> dflt,
        s"""{"type": "<TYPE>", "mean" : 0}""" -> dflt,
        s"""{"type": "<TYPE>", "mean" : 0.0}""" -> dflt,
        s"""{"type": "<TYPE>", "mean" : "0"}""" -> dflt,
        s"""{"type": "<TYPE>", "mean" : "0.0"}""" -> dflt,
        s"""{"type": "<TYPE>", "stddev" : $stddev}""" -> dflt,
        s"""{"type": "<TYPE>", "stddev" : "$stddev"}""" -> dflt,
        s"""{"type": "<TYPE>", "stddev" : $stddev.0}""" -> dflt,
        s"""{"type": "<TYPE>", "stddev" : "$stddev.0"}""" -> dflt,
        s"""{"type": "<TYPE>", "mean": 0, "faker": {"stddev": $stddev}}""" -> dflt
      )

      aNumber(
        "should generate numbers centered around 100, with 95% of the values between `80` and `120`, with possible negatives"
      )(
        // Note that it is *extremely* unlikely that a value outside 10 standard deviations would ever occur.
        """{"type": "<TYPE>", "mean": 100, "stddev": 10}""" -> gauss100_10,
        """{"type": "<TYPE>", "mean": 100, "faker": {"stddev": 10}}""" -> gauss100_10,
        """{"type": "<TYPE>", "stddev": 10, "faker": {"mean": 100}}""" -> gauss100_10,
        """{"type": "<TYPE>", "faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10
      )

      aNumber(
        "should generate numbers centered around 100, with 95% of the values between `80` and `120`, guaranteed no negatives"
      )(
        // But here it is impossible
        """{"type": "<TYPE>", "min": 0, "faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10
      )

      aNumber("should apply a filter to only return numbers greater than the mean")(
        """{"type": "<TYPE>", "min": 100, "faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10NonNeg
        // Be careful about setting a valid interval that is unlikely to have a generated number.  This next example
        // requires generated values to be 5 standard deviations from the main, so only one out of every million
        // generated values will be retained.
        // """{"type": "<TYPE>", "max": 50, "gauss": {"mean": 100, "stddev": 10}}"""
      )
    }
  }
  gaussStrategy(Tester.Int)
  gaussStrategy(Tester.Long)
  gaussStrategy(Tester.Float)
  gaussStrategy(Tester.Double)

  def sequenceStrategy[T](helper: NumericTester[T]): Unit = {
    describe(s"Generate ${helper.sType} with the sequence strategy") {
      val aNumber = helper.apply _

      aNumber("should generate a simple sequence")(
        """{"type": "<TYPE>", "faker": "sequence"}""" -> (0 to 9),
        """{"type": "<TYPE>", "step": 1}""" -> (0 to 9)
      )
      aNumber("should start from a specific value")(
        """{"type": "<TYPE>", "start": 10000}""" -> (10000 to 10009)
      )
      aNumber("should start from a specific value and step")(
        """{"type": "<TYPE>", "start": 10, "step": 2}""" -> (10 to 28 by 2)
      )
      aNumber("should count down with a negative step")(
        """{"type": "<TYPE>", "step": -1, "max": 4}""" -> Seq(3, 2, 1, 0, -1, -2),
        """{"type": "<TYPE>", "step": -1, "min": 0, "max": 4}""" -> Seq(3, 2, 1, 0, 3, 2)
      )
      aNumber("should look like a countdown down when you loop every step")(
        """{"type": "<TYPE>", "start": 3, "min": 0, "max": 4, "step": 3}""" -> Seq(3, 2, 1, 0, 3, 2)
      )
      aNumber("should start from a specific value and loop")(
        """{"type": "<TYPE>", "min": 0,  "faker": {"start": 10, "max": 13}}""" -> Seq(10, 11, 12, 0, 1),
        """{"type": "<TYPE>", "min": 0, "max": 13, "faker": {"start": 10}}""" -> Seq(10, 11, 12, 0, 1)
      )

      if (helper.sType == Schema.Type.INT)
        aNumber("should roll over at the default max value")(
          s"""{"type": "<TYPE>", "step": 1, "start": ${Int.MaxValue - 5}}""" -> Seq(2147483642, 2147483643, 2147483644,
            2147483645, 2147483646, -2147483648, -2147483647, -2147483646, -2147483645, -2147483644)
        )
      else if (helper.sType == Schema.Type.LONG)
        aNumber("should roll over at the default max value")(
          s"""{"type": "<TYPE>", "step": 1, "start": ${Long.MaxValue - 5}}""" -> Seq(9223372036854775802L,
            9223372036854775803L, 9223372036854775804L, 9223372036854775805L, 9223372036854775806L,
            -9223372036854775808L, -9223372036854775807L, -9223372036854775806L, -9223372036854775805L,
            -9223372036854775804L)
        )
      else if (helper.sType == Schema.Type.FLOAT)
        aNumber("should go to infinity at the default max value")(
          s"""{"type": "<TYPE>", "step": ${Float.MaxValue * 0.01}, "start": ${Float.MaxValue * 0.95}}""" -> Seq(
            3.2326822e38,
            3.2667104e38,
            3.3007389e38,
            3.334767e38,
            3.3687953e38,
            3.4028235e38,
            Float.PositiveInfinity,
            Float.PositiveInfinity,
            Float.PositiveInfinity,
            Float.PositiveInfinity
          )
        )
      else if (helper.sType == Schema.Type.DOUBLE)
        aNumber("should go to infinity at the default max value")(
          s"""{"type": "<TYPE>", "step": ${Double.MaxValue * 0.01}, "start": ${Double.MaxValue * 0.95}}""" -> Seq(
            1.7078084781191998e308,
            1.725785409467823e308,
            1.7437623408164462e308,
            1.7617392721650694e308,
            1.7797162035136925e308,
            1.7976931348623157e308,
            Double.NaN, // TODO
            Double.NaN,
            Double.NaN,
            Double.NaN
          )
        )
    }
  }
  sequenceStrategy(Tester.Int)
  sequenceStrategy(Tester.Long)
  sequenceStrategy(Tester.Float)
  sequenceStrategy(Tester.Double)

  def valueStrategy[T](helper: NumericTester[T]): Unit = {
    describe(s"Generate ${helper.sType} with the value strategy") {
      val aNumber = helper.apply _

      aNumber("should generate a constant value")(
        // These are all equivalent
        """{"type": "<TYPE>", "value": 123}""" -> Seq.fill(10)(123),
        """{"type": "<TYPE>", "faker": 123}""" -> Seq.fill(10)(123),
        """{"type": "<TYPE>", "faker": "value", "value": 123}""" -> Seq.fill(10)(123),
        """{"type": "<TYPE>", "value": 123.9}""" -> Seq.fill(10)(if (helper.isIntegral) 123 else 123.9),
        """{"type": "<TYPE>", "value": "123.1"}""" -> Seq.fill(10)(if (helper.isIntegral) 123 else 123.1)
      )

      aNumber("should apply the minimum and maximum to the constant value")(
        """{"type": "<TYPE>", "min": 0, "value": 123}""" -> Seq.fill(10)(123),
        """{"type": "<TYPE>", "min": 234, "value": 123}""" -> Seq.fill(10)(234),
        """{"type": "<TYPE>", "max": 0, "value": 123}""" -> Seq.fill(10)(0),
        """{"type": "<TYPE>", "max": 234, "value": 123}""" -> Seq.fill(10)(123)
      )

      for (maxValue <- Seq("256", "256.1", "256.999")) {
        val expected = Seq.fill(10)(if (helper.isIntegral) 256 else maxValue)
        aNumber(s"should support the max and min value: $maxValue and \"$maxValue\"")(
          s"""{"type": "<TYPE>", "value": 999, "max": $maxValue}""" -> expected,
          s"""{"type": "<TYPE>", "value": 999, "max": "$maxValue"}""" -> expected,
          s"""{"type": "<TYPE>", "value": 123, "min": $maxValue}""" -> expected,
          s"""{"type": "<TYPE>", "value": 123, "min": "$maxValue"}""" -> expected
        )
      }
    }
  }
  valueStrategy(Tester.Int)
  valueStrategy(Tester.Long)
  valueStrategy(Tester.Float)
  valueStrategy(Tester.Double)

  describe("Generate INT with the oneof strategy") {
    it("should pick a random value between 123, 234 and 345") {
      // These are all equivalent
      val expected = Seq(123, 234, 234, 345, 345, 345, 345, 123, 123, 345)
      Tester.Int.generate("""{"type": "int", "faker": [123, 234, 345]}""").take(10) shouldBe expected
      Tester.Int.generate("""{"type": "int", "oneof": [123, 234, 345]}""").take(10) shouldBe expected
    }

    it("should pick randomly between a single digit and 999") {
      Tester.Int.generate("""{"type": "int", "faker": [999, {"min": 0, "max": 9}]}""").take(10) shouldBe Seq(7, 999, 8,
        999, 3, 2, 999, 999, 999, 8)
    }

    it("should always generate 123") {
      val expected = Seq.fill(10)(123)
      Tester.Int.generate("""{"type": "int", "oneof": [123, 321, 999], "index": 0}""").take(10) shouldBe expected
      Tester.Int.generate("""{"type": "int", "oneof": 123}""").take(10) shouldBe expected
      Tester.Int.generate("""{"type": "int", "oneof": [123, 321, 999], "index": -1}""").take(10) shouldBe expected
    }

    it("should cycle through the elements") {
      Tester.Int.generate("""{"type": "int", "oneof": [123, 321, 999], "index": {"step": 1}}""").take(10) shouldBe
        Seq(123, 321, 999, 123, 321, 999, 123, 321, 999, 123)
    }

    it("should pick a gaussian distribution of elements") {
      Tester.Int
        .generate("""{"type": "int", "oneof": [1,2,3,4,5], "index": {"mean": 2.5, "stddev": 1}}""")
        .take(1000)
        .groupBy(identity)
        .view
        .mapValues(_.size)
        .toMap shouldBe Map(1 -> 86, 2 -> 207, 3 -> 386, 4 -> 251, 5 -> 70)
    }
  }

  describe("Generate INT with aggregate strategies") {
    it("should sum values from multiple strategies") {
      Tester.Int.generate("""{"type": "int", "sumof": [1,2,3,4,5], "faker": "sumof"}""").take(10) shouldBe Seq.fill(10)(
        15
      )
      Tester.Int.generate("""{"type": "int", "sumof": [1,2,3,4,5]}""").take(10) shouldBe Seq.fill(10)(15)
    }
    it("should multiply values from multiple strategies") {
      Tester.Int.generate("""{"type": "int", "productof": [1,2,3,4,5]}""").take(10) shouldBe Seq.fill(10)(120)
    }
    it("should find the minimum from multiple strategies") {
      Tester.Int.generate("""{"type": "int", "minof": [1,2,3,4,5]}""").take(10) shouldBe Seq.fill(10)(1)
    }
    it("should find the maximum from multiple strategies") {
      Tester.Int.generate("""{"type": "int", "maxof": [1,2,3,4,5]}""").take(10) shouldBe Seq.fill(10)(5)
    }
    it("should find the average from multiple strategies") {
      Tester.Int.generate("""{"type": "int", "meanof": [1,2,3,4,5]}""").take(10) shouldBe Seq.fill(10)(3)
    }
  }

  describe("Generating Avro RECORD data") {
    it("should create ten character strings by default") {
      val schema = SchemaBuilder
        .record("Example")
        .fields()
        .name("id")
        .`type`(IntSequence)
        .noDefault()
        .requiredString("name")
        .endRecord()
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(schema)
      gen.apply(ctx) shouldBe new GenericRecordBuilder(schema).set("id", 0).set("name", "CCzLNHBFHu").build()
      gen.apply(ctx) shouldBe new GenericRecordBuilder(schema).set("id", 1).set("name", "RvbI1iI19W").build()
      gen.apply(ctx) shouldBe new GenericRecordBuilder(schema).set("id", 2).set("name", "jGGR8UNWut").build()
    }
  }

  describe("Generating Avro ENUM data") {
    it("should pick symbols randomly") {
      val schema = SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E")
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(schema)
      gen.apply(ctx) shouldBe "A"
      gen.apply(ctx) shouldBe "D"
      gen.apply(ctx) shouldBe "E"
    }

    it("should pick symbols sequentially") {
      Tester.String
        .generate(SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E"), ArgStep -> 1)
        .take(10) shouldBe Seq("A", "B", "C", "D", "E", "A", "B", "C", "D", "E")
    }

    it("should pick symbols sequentially with a step") {
      Tester.String
        .generate(
          SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E"),
          ArgStep -> 3
        )
        .take(10) shouldBe Seq("A", "D", "B", "E", "C", "A", "D", "B", "E", "C")
    }
  }

  describe("Generating Avro ARRAY data") {
    it("should create arrays of its element type") {
      val schema = SchemaBuilder.array().items().stringBuilder().endString()
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(schema)
      gen(ctx) shouldBe Array("CzLNHBFHuR", "vbI1iI19Wj")
      gen(ctx) shouldBe Array("GR8UNWutFR", "ZvWebpA5WH")
      gen(ctx) shouldBe Array("yqts0coJXQ", "qPyuxbr589", "wyJzS2SuiH", "rAOB2RuvBb")
    }
  }

  describe("Generating Avro MAP data") {
    it("should create maps of its value type") {
      val schema = SchemaBuilder.map().values().`type`(IntSequence)
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(schema)
      gen(ctx) shouldBe Map("CzLNHBFHuR" -> 0, "vbI1iI19Wj" -> 1)
      gen(ctx) shouldBe Map("GR8UNWutFR" -> 2, "ZvWebpA5WH" -> 3)
      gen(ctx) shouldBe Map("yqts0coJXQ" -> 4, "qPyuxbr589" -> 5, "wyJzS2SuiH" -> 6, "rAOB2RuvBb" -> 7)
    }
  }

  describe("Generating Avro UNION data") {
    it("should generate data from the union types with equal probability") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.createUnion(Schema.create(Schema.Type.NULL), IntSequence))
      gen(ctx) shouldBe 0
      gen(ctx) shouldBe 1
      Option(gen(ctx)) shouldBe None
      gen(ctx) shouldBe 2
      gen(ctx) shouldBe 3
      Option(gen(ctx)) shouldBe None
      gen(ctx) shouldBe 4
      Option(gen(ctx)) shouldBe None
      gen(ctx) shouldBe 5
    }
  }

  describe("Generating Avro FIXED data") {
    it("should create byte arrays by default") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.createFixed("Example", "", "", 4))
      gen(ctx) shouldBe Array(96, -76, 32, -69)
      gen(ctx) shouldBe Array(56, 81, -39, -44)
      gen(ctx) shouldBe Array(122, -53, -109, 61)
    }
  }

  describe("Generating Avro STRING data") {
    it("should create ten character strings by default") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.create(Schema.Type.STRING))
      gen(ctx) shouldBe "CCzLNHBFHu"
      gen(ctx) shouldBe "RvbI1iI19W"
      gen(ctx) shouldBe "jGGR8UNWut"
    }

    it("should have a configurable length") {
      Tester.String.generate(ArgLength -> 5).take(5) shouldBe Seq("CCzLN", "HBFHu", "RvbI1", "iI19W", "jGGR8")
    }

    /** Generate a list from a faker expression. */
    def genFakeString(expression: String): LazyList[String] = Tester.String.generate(ArgExpression -> expression)

    it("should create faker data") {
      // Name examples
      genFakeString("#{Name.first_name} #{Name.last_name}").take(3) shouldBe Seq(
        "Kit Graham",
        "Dessie McDermott",
        "Carola Runolfsson"
      )
      // Address examples
      genFakeString("#{Address.street_address}\n#{Address.city}, #{Address.country}").take(3) shouldBe Seq(
        "95986 Langworth Bypass\nCarolahaven, Romania",
        "996 Thi Circle\nEast Jeanfort, New Caledonia",
        "1324 O'Reilly Lane\nBernardville, Sweden"
      )
    }
  }

  describe("Generating Avro BYTES data") {
    it("should create variable byte arrays by default") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.create(Schema.Type.BYTES))
      gen(ctx) shouldBe Array(56, 81, -39, -44, 122)
      gen(ctx) shouldBe Array(-10, -55, 45, -93, 58, -16, 29)
      gen(ctx) shouldBe Array(3, 37, -12, 29, 62, -70)
    }
  }

  // LONG and INT schema types are identical except for the return type
  for (tester <- Seq(Tester.Int, Tester.Long))
    describe(s"Generating Avro ${tester.sType} data (common)") {
      import tester._
      it("should be random by default") {
        generate(ArgMin -> -1, ArgMax -> 2).take(10) shouldBe Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
      }

      it("should start at the specified value") {
        generate(ArgMin -> "10", ArgStep -> "1").take(10) shouldBe Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
      }

      it("should rotate before reaching the end") {
        generate(ArgMin -> 1, ArgMax -> 4, ArgStep -> 1).take(10) shouldBe Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
      }

      it("should have a configurable step before reaching the end") {
        generate(ArgMin -> -5, ArgMax -> 5, ArgStep -> 3).take(15) shouldBe Seq(-5, -2, 1, 4, -3, 0, 3, -4, -1, 2, -5,
          -2, 1, 4, -3)
      }
    }

  // DOUBLE and FLOAT schema types are identical except for the precision.  The type is checked in the generate method, so we force it to a Double for the value check
  for ((tester, precision) <- Seq((Tester.Float, 1e-6), (Tester.Double, 1e-14)))
    describe(s"Generating Avro ${tester.sType} data (common)") {
      import tester._
      it("should generate random numbers") {
        val gen = generate().map(_.toString.toDouble)
        gen.head shouldBe 0.730967787376657 +- precision
        gen(1) shouldBe 0.24053641567148587 +- precision
        gen(2) shouldBe 0.6374174253501083 +- precision
      }

      it("should generate random numbers with a minimum and maximum") {
        val gen = generate(AvroFaker.ArgMin -> 10, AvroFaker.ArgMax -> 20).map(_.toString.toDouble)
        gen.head shouldBe 17.30967787376657 +- precision
        gen(1) shouldBe 12.405364156714858 +- precision
        gen(2) shouldBe 16.37417425350108 +- precision
      }

      it("should generate guassian distribution") {
        val gen = generate(AvroFaker.ArgMean -> 0.0).map(_.toString.toDouble)
        gen.head shouldBe 0.8025330637390305 +- precision
        gen(1) shouldBe -0.9015460884175122 +- precision
        gen(2) shouldBe 2.080920790428163 +- precision
      }

      it("should start at the minimum") {
        generate(ArgMin -> 10, ArgStep -> 1).map(_.toString.toDouble).take(10) shouldBe Seq(10d, 11d, 12d, 13d, 14d,
          15d, 16d, 17d, 18d, 19d)
      }

      it("should rotate before reaching the end") {
        generate(ArgMin -> 1, ArgMax -> 4, ArgStep -> 1).map(_.toString.toDouble).take(10) shouldBe Seq(1d, 2d, 3d, 1d,
          2d, 3d, 1d, 2d, 3d, 1d)
      }

      it("should start at the specified value at fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(ArgMax -> 0.75, ArgStep -> -0.25).map(_.toString.toDouble).take(10) shouldBe Seq(0.5, 0.25, 0.0, -0.25,
          -0.5, -0.75, -1.0, -1.25, -1.5, -1.75)
      }

      it("should rotate before reaching the end with fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(ArgMin -> 0.75, ArgMax -> 2.5, ArgStep -> 0.5)
          .map(_.toString.toDouble)
          .take(10) shouldBe Seq(0.75, 1.25, 1.75, 2.25, 1.0, 1.5, 2.0, 0.75, 1.25, 1.75)
      }

      it("should have a configurable step before reaching the end") {
        generate(ArgMin -> -5, ArgMax -> 5, ArgStep -> 3)
          .map(_.toString.toDouble)
          .take(15) shouldBe Seq(-5d, -2d, 1d, 4d, -3d, 0d, 3d, -4d, -1d, 2d, -5d, -2d, 1d, 4d, -3d)
      }
    }

  describe("Generating Avro BOOLEAN data") {
    it("should generate random booleans") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.create(Schema.Type.BOOLEAN))
      gen(ctx) shouldBe true
      gen(ctx) shouldBe true
      gen(ctx) shouldBe false
    }
  }

  describe("Generating Avro NULL data") {
    it("should only ever generate NULL") {
      val ctx = FakerContext(new Random(0))
      val gen = AvroFaker(Schema.create(Schema.Type.NULL))
      Option(gen(ctx)) shouldBe None
      Option(gen(ctx)) shouldBe None
      Option(gen(ctx)) shouldBe None
    }
  }
}
