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

  val IntSequence: Schema = new Schema.Parser().parse("""{"type": "int", "step": 1}""")

  /** This tester helps run suites by generating data.
    *
    * @param sType
    *   The Avro numeric Schema.Type
    * @tparam T
    *   The type of datum that AvroFaker should be generating
    */
  class Tester[T](val sType: Schema.Type)(implicit ct: ClassTag[T]) {

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
      case _ if schema.contains("<TYPE>") => schema.replace("<TYPE>", sType.toString.toLowerCase())
      case _ if schema.startsWith("{") && !schema.contains("\"type\":") =>
        s"""{"type": "${sType.toString.toLowerCase}", ${schema.substring(1)}"""
      case _ => schema
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

    case class ItTestExpected(description: String, schema: String, fn: Any => Any = identity) {

      /** Produces the test to be executed (an `it` word). */
      def execute(expected: Seq[_]): Unit = {
        // Generate the values and ensure they are the correct type at the generator
        val values = generate(schema).take(expected.size)

        it(s"$description: $schema") {
          values shouldBe expected.map(fn)
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

    class Applies(description: String) {
      def apply(schema: String, dist: Dist): Unit =
        dist.execute(description, adaptSchemaWithType(schema), fn = identity)

      def apply(schema: String, expected: Seq[_]): Unit =
        ItTestExpected(description, adaptSchemaWithType(schema)).execute(expected)

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
      extends Tester[T](sType)(ct) {

    /** True if we are matching a whole number type, false for floating point. */
    val isIntegral: Boolean = num match {
      case _: Integral[T] => true
      case _              => false
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

    class NumericApplies(description: String) extends Applies(description) {
      override def apply(schema: String, dist: Dist): Unit = {
        if (isIntegral)
          dist.execute(description, adaptSchemaWithType(schema), toLong)
        else
          dist.execute(description, adaptSchemaWithType(schema), toDouble)
      }

      override def apply(schema: String, expected: Seq[_]): Unit = {
        if (isIntegral)
          ItTestExpected(description, adaptSchemaWithType(schema), toLong).execute(expected)
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
    val String = new Tester[String](Schema.Type.STRING)
  }

  def randomStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the random strategy") {

      /** Expected results over the default random range */
      val random = it.sType match {
        case Schema.Type.INT =>
          Seq(-1630935619, -1483802595, -864264928)
        case Schema.Type.LONG =>
          Seq(-4962768465676381896L, 4437113781045784766L, -6688467811848818630L)
        case _ =>
          Seq(0.730967787376657, 0.24053641567148587, 0.6374174253501083)
      }

      /** Expected results over the default positive range */
      val withMinimum = it.sType match {
        case Schema.Type.INT =>
          Seq(711764125, 1302116448, 663681053)
        case Schema.Type.LONG =>
          Seq(1340999404015745395L, 4053417136674644310L, 8448822068277548669L)
        case _ =>
          Seq(0.8654838936883285, 0.6202682078357429, 0.8187087126750541)
      }

      /** Expected results over [0, 256) */
      val byteSize = it.sType match {
        case Schema.Type.INT | Schema.Type.LONG =>
          Seq(187, 212, 61, 155, 163, 79, 140, 29, 152, 200)
        case _ =>
          Seq(187.1277535684242, 61.57732241190038, 163.1788608896277, 140.9118733101143, 152.97159111608366,
            85.30391026602234, 98.60843129362394, 252.1194342911511, 225.07072457535492, 240.95978994742129)
      }

      it("should use the random strategy on unannotated schemas")(
        """"<TYPE>"""" -> random,
        """{"type": "<TYPE>"}""" -> random
      )

      it("should generate a random number when the random strategy is explicitly set")(
        """{"faker": "random"}""" -> random,
        """{"faker": {"faker": "random"}}""" -> random
      )

      it("should generate a random number when the random strategy is unset")(
        """{"faker": {}}""" -> random
      )

      if (it.isIntegral) {
        it("should allow configuring the random strategy with a minimum")(
          """{"min": 0, "faker": "random"}""" -> withMinimum,
          """{"faker": {"min": 0}}""" -> withMinimum
        )

        it("should implicitly configure the random strategy from the schema annotations")(
          """{"min": 0}""" -> withMinimum
        )

        it("should override arguments from the schema annotations")(
          """{"min": 100, "faker": {"min": 0}}""" -> withMinimum
        )
      } else {
        it("should allow configuring the random strategy with a minimum")(
          """{"min": 0.5, "faker": "random"}""" -> withMinimum,
          """{"faker": {"min": 0.5}}""" -> withMinimum
        )

        it("should implicitly configure the random strategy from the schema annotations")(
          """{"min": 0.5}""" -> withMinimum
        )

        it("should override arguments from the schema annotations")(
          """{"min": -0.5, "faker": {"min": 0.5}}""" -> withMinimum
        )
      }

      it("should be configurable by the min and the max")(
        """{"min": 0, "max": 256}""" -> byteSize,
        """{"faker": {"min": 0, "max": 256}}""" -> byteSize,
        """{"min": 0, "faker": {"max": 256}}""" -> byteSize,
        """{"min": 100, "faker": {"min": 0, "max": 256}}""" -> byteSize
      )
    }
  }
  randomStrategy(Tester.Int)
  randomStrategy(Tester.Long)
  randomStrategy(Tester.Float)
  randomStrategy(Tester.Double)

  def gaussStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the gauss strategy") {
      val stddev = if (it.isIntegral) "100" else "1"

      /** Expected results over the default gauss range */
      val dflt = it.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(80, -90, 208, 76, 98, -168, -2, 11, -39, -64)
        case _ =>
          Seq(0.8025330637390305, -0.9015460884175122, 2.080920790428163, 0.7637707684364894, 0.9845745328825128,
            -1.6834122587673428, -0.027290262907887285, 0.11524570286202315, -0.39016704137993785, -0.6433888131264491)
      }

      val gauss100_10 = it.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(108, 90, 120, 107, 109, 83, 99, 101, 96, 93)
        case _ =>
          Seq(108.02533063739031, 90.98453911582487, 120.80920790428164, 107.6377076843649, 109.84574532882513,
            83.16587741232658, 99.72709737092113, 101.15245702862023, 96.09832958620062, 93.5661118687355)
      }

      val gauss100_10NonNeg = it.sType match {
        case Schema.Type.INT | Schema.Type.LONG => Seq(108, 120, 107, 109, 101, 100, 105, 102, 114, 102)
        case _ =>
          Seq(108.02533063739031, 120.80920790428164, 107.6377076843649, 109.84574532882513, 101.15245702862023,
            100.52460907198835, 105.21342076929889, 102.60718194028357, 114.03147381720936, 102.71130617070202)
      }

      it(
        "should generate between a bell curve of numbers centered around 0, with 95% of the values between -200 and 200"
      )(
        // These are all equivalent
        s"""{"faker": "gauss"}""" -> dflt,
        s"""{"mean" : 0}""" -> dflt,
        s"""{"mean" : 0.0}""" -> dflt,
        s"""{"mean" : "0"}""" -> dflt,
        s"""{"mean" : "0.0"}""" -> dflt,
        s"""{"stddev" : $stddev}""" -> dflt,
        s"""{"stddev" : "$stddev"}""" -> dflt,
        s"""{"stddev" : $stddev.0}""" -> dflt,
        s"""{"stddev" : "$stddev.0"}""" -> dflt,
        s"""{"mean": 0, "faker": {"stddev": $stddev}}""" -> dflt
      )

      it(
        "should generate numbers centered around 100, with 95% of the values between `80` and `120`, with possible negatives"
      )(
        // Note that it is *extremely* unlikely that a value outside 10 standard deviations would ever occur.
        """{"mean": 100, "stddev": 10}""" -> gauss100_10,
        """{"mean": 100, "faker": {"stddev": 10}}""" -> gauss100_10,
        """{"stddev": 10, "faker": {"mean": 100}}""" -> gauss100_10,
        """{"faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10
      )

      it(
        "should generate numbers centered around 100, with 95% of the values between `80` and `120`, guaranteed no negatives"
      )(
        // But here it is impossible
        """{"min": 0, "faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10
      )

      it("should apply a filter to only return numbers greater than the mean")(
        """{"min": 100, "faker": {"mean": 100, "stddev": 10}}""" -> gauss100_10NonNeg
        // Be careful about setting a valid interval that is unlikely to have a generated number.  This next example
        // requires generated values to be 5 standard deviations from the main, so only one out of every million
        // generated values will be retained.
        // """{"max": 50, "gauss": {"mean": 100, "stddev": 10}}"""
      )
    }
  }
  gaussStrategy(Tester.Int)
  gaussStrategy(Tester.Long)
  gaussStrategy(Tester.Float)
  gaussStrategy(Tester.Double)

  def sequenceStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the sequence strategy") {

      it("should generate a simple sequence")(
        """{"faker": "sequence"}""" -> (0 to 9),
        """{"step": 1}""" -> (0 to 9)
      )
      it("should start from a specific value")(
        """{"start": 10000}""" -> (10000 to 10009)
      )
      it("should start from a specific value and step")(
        """{"start": 10, "step": 2}""" -> (10 to 28 by 2)
      )
      it("should count down with a negative step")(
        """{"step": -1, "max": 4}""" -> Seq(3, 2, 1, 0, -1, -2),
        """{"step": -1, "min": 0, "max": 4}""" -> Seq(3, 2, 1, 0, 3, 2)
      )
      it("should look like a countdown down when you loop every step")(
        """{"start": 3, "min": 0, "max": 4, "step": 3}""" -> Seq(3, 2, 1, 0, 3, 2)
      )
      it("should start from a specific value and loop")(
        """{"min": 0,  "faker": {"start": 10, "max": 13}}""" -> Seq(10, 11, 12, 0, 1),
        """{"min": 0, "max": 13, "faker": {"start": 10}}""" -> Seq(10, 11, 12, 0, 1)
      )

      if (it.sType == Schema.Type.INT)
        it("should roll over at the default max value")(
          s"""{"step": 1, "start": ${Int.MaxValue - 5}}""" -> Seq(2147483642, 2147483643, 2147483644, 2147483645,
            2147483646, -2147483648, -2147483647, -2147483646, -2147483645, -2147483644)
        )
      else if (it.sType == Schema.Type.LONG)
        it("should roll over at the default max value")(
          s"""{"step": 1, "start": ${Long.MaxValue - 5}}""" -> Seq(9223372036854775802L, 9223372036854775803L,
            9223372036854775804L, 9223372036854775805L, 9223372036854775806L, -9223372036854775808L,
            -9223372036854775807L, -9223372036854775806L, -9223372036854775805L, -9223372036854775804L)
        )
      else if (it.sType == Schema.Type.FLOAT)
        it("should go to infinity at the default max value")(
          s"""{"step": ${Float.MaxValue * 0.01}, "start": ${Float.MaxValue * 0.95}}""" -> Seq(
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
      else if (it.sType == Schema.Type.DOUBLE)
        it("should go to infinity at the default max value")(
          s"""{"step": ${Double.MaxValue * 0.01}, "start": ${Double.MaxValue * 0.95}}""" -> Seq(
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

  def valueStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the value strategy") {

      it("should generate a constant value")(
        // These are all equivalent
        """{"value": 123}""" -> Seq.fill(10)(123),
        """{"faker": 123}""" -> Seq.fill(10)(123),
        """{"faker": "value", "value": 123}""" -> Seq.fill(10)(123),
        """{"value": 123.9}""" -> Seq.fill(10)(if (it.isIntegral) 123 else 123.9),
        """{"value": "123.1"}""" -> Seq.fill(10)(if (it.isIntegral) 123 else 123.1)
      )

      it("should apply the minimum and maximum to the constant value")(
        """{"min": 0, "value": 123}""" -> Seq.fill(10)(123),
        """{"min": 234, "value": 123}""" -> Seq.fill(10)(234),
        """{"max": 0, "value": 123}""" -> Seq.fill(10)(0),
        """{"max": 234, "value": 123}""" -> Seq.fill(10)(123)
      )

      for (maxValue <- Seq("256", "256.1", "256.999")) {
        val expected = Seq.fill(10)(if (it.isIntegral) 256 else maxValue)
        it(s"should support the max and min value: $maxValue and \"$maxValue\"")(
          s"""{"value": 999, "max": $maxValue}""" -> expected,
          s"""{"value": 999, "max": "$maxValue"}""" -> expected,
          s"""{"value": 123, "min": $maxValue}""" -> expected,
          s"""{"value": 123, "min": "$maxValue"}""" -> expected
        )
      }
    }
  }
  valueStrategy(Tester.Int)
  valueStrategy(Tester.Long)
  valueStrategy(Tester.Float)
  valueStrategy(Tester.Double)

  def oneofStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the oneof strategy") {

      /** Expected results when picking from a list of values. */
      val values = Seq(123, 234, 234, 345, 345, 345, 345, 123, 123, 345)

      it("should pick a random value between 123, 234 and 345")(
        // These are all equivalent
        """{"faker": [123, 234, 345]}""" -> values,
        """{"oneof": [123, 234, 345]}""" -> values
      )

      /** Expected results when picking from a list of values. */
      val multistrategy =
        if (it.isIntegral) Seq(7, 999, 8, 999, 3, 2, 999, 999, 999, 8)
        else
          Seq(7.48296889908355, 5.736756828150974, 1.053059479265026, 2.9989655952898477, 999.0, 8.863573861798281,
            0.20773667799297957, 999.0, 999.0, 999.0)

      it("should pick randomly between a single digit and 999")(
        """{"faker": [999, {"min": 0, "max": 9}]}""" -> multistrategy
      )

      it("should always generate 123")(
        """{"oneof": [123, 321, 999], "index": 0}""" -> Seq.fill(10)(123),
        """{"oneof": 123}""" -> Seq.fill(10)(123),
        """{"oneof": [123, 321, 999], "index": -1}""" -> Seq.fill(10)(123)
      )

      it("should cycle through the elements")(
        """{"oneof": [123, 321, 999], "index": {"step": 1}}""" -> Seq(123, 321, 999, 123, 321, 999, 123, 321, 999, 123)
      )

      it("should pick a gaussian distribution of elements").apply(
        """{"oneof": [1,2,3,4,5], "index": {"mean": 2.5, "stddev": 1}}""",
        new it.Dist(1 -> 86, 2 -> 207, 3 -> 386, 4 -> 251, 5 -> 70)
      )
    }
  }
  oneofStrategy(Tester.Int)
  oneofStrategy(Tester.Long)
  oneofStrategy(Tester.Float)
  oneofStrategy(Tester.Double)

  def aggregateStrategies[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with other aggregate strategies") {

      it("should sum values from multiple strategies")(
        """{"sumof": [1,2,3,4,5], "faker": "sumof"}""" -> Seq.fill(10)(15),
        """{"sumof": [1,2,3,4,5]}""" -> Seq.fill(10)(15)
      )
      it("should multiply values from multiple strategies")(
        """{"productof": [1,2,3,4,5]}""" -> Seq.fill(10)(120)
      )
      it("should find the minimum from multiple strategies")(
        """{"minof": [1,2,3,4,5]}""" -> Seq.fill(10)(1)
      )
      it("should find the maximum from multiple strategies")(
        """{"maxof": [1,2,3,4,5]}""" -> Seq.fill(10)(5)
      )
      it("should find the average from multiple strategies")(
        """{"meanof": [1,2,3,4,5]}""" -> Seq.fill(10)(3)
      )
    }
  }
  aggregateStrategies(Tester.Int)
  aggregateStrategies(Tester.Long)
  aggregateStrategies(Tester.Float)
  aggregateStrategies(Tester.Double)

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

  describe("Generate STRING with the random strategy") {
    val it = Tester.String

    val random = Seq(
      "CCzLNHBFHu",
      "RvbI1iI19W",
      "jGGR8UNWut",
      "FRZvWebpA5",
      "WHfyqts0co",
      "JXQqPyuxbr",
      "589wyJzS2S",
      "uiHrAOB2Ru"
    )

    val random5to9 = Seq("CzLNH", "FHuRvb", "1iI19Wj", "GR8UNWut", "RZvWe", "pA5WH", "yqts0", "oJXQqPy")

    it("should use the random strategy on unannotated schemas")(
      """"<TYPE>"""" -> random,
      """{"type": "<TYPE>"}""" -> random,
      """{"type": "<TYPE>", "length": 10}""" -> random
    )

    it("should generate a random string when the random strategy is explicitly set")(
      """{"faker": "random"}""" -> random,
      """{"faker": "random", "length": 10}""" -> random,
      """{"faker": {"faker": "random"}}""" -> random
    )

    it("should generate a random String when the random strategy is unset")(
      """{"faker": {}}""" -> random
    )

    it("should allow configuring the random strategy with a length")(
      """{"length": 5}""" -> random.flatMap(_.splitAt(5).productIterator), // Same seed gets the same letters
      """{"length": 1}""" -> random.flatMap(_.toCharArray).map(_.toString),
      """{"length": {"min": 5, "max": 10}}""" -> random5to9,
      """{"min": 5, "length": {"max": 10}}""" -> random5to9,
      """{"min": 100, "length": {"min": 5, "max": 10}}""" -> random5to9,
      """{"min": 5, "max": 10, "length": {}}""" -> random5to9,
      """{"min": 5, "max": 10}""" -> random5to9
    )
  }

  describe(s"Generate STRING with the value, oneof and sumof strategies") {
    Tester.String("should generate a constant value")(
      // These are all equivalent
      """{"value": "ABC"}""" -> Seq.fill(10)("ABC"),
      """{"faker": 123}""" -> Seq.fill(10)("123"),
      """{"faker": "value", "value": "ABC"}""" -> Seq.fill(10)("ABC")
    )

    val randoms = Seq("ABC", "BCD", "BCD", "CDE", "CDE", "CDE", "CDE", "ABC", "ABC", "CDE")

    Tester.String("should pick a random value between 123, 234 and 345")(
      // These are all equivalent
      """{"faker": ["ABC", "BCD", "CDE"]}""" -> randoms,
      """{"value": ["ABC", "BCD", "CDE"]}""" -> randoms,
      """{"oneof": ["ABC", "BCD", "CDE"]}""" -> randoms,
      """{"faker": "value", "value": ["ABC", "BCD", "CDE"]}""" -> randoms,
      """{"faker": "oneof", "oneof": ["ABC", "BCD", "CDE"]}""" -> randoms
    )

    /** Expected results when picking from a list of values. */
    val multistrategy =
      Seq("U721", "J403", "Z360", ":::", "Z430", ":::", "Z082", "F835", ":::", ":::")

    Tester.String("should pick randomly between ::: and the default 4-digit expression")(
      """{"faker": [":::", {"faker": "expression"}]}""" -> multistrategy
    )

    Tester.String("should always generate ABC")(
      """{"oneof": ["ABC", "BCD", "CDE"], "index": 0}""" -> Seq.fill(10)("ABC"),
      """{"oneof": "ABC"}""" -> Seq.fill(10)("ABC"),
      """{"oneof": ["ABC", "BCD", "CDE"], "index": -1}""" -> Seq.fill(10)("ABC")
    )

    Tester.String("should cycle through the elements")(
      """{"oneof": ["ABC", "BCD", "CDE"], "index": {"step": 1}}""" -> Seq("ABC", "BCD", "CDE", "ABC", "BCD", "CDE")
    )

    Tester.String("should pick a gaussian distribution of elements")(
      """{"oneof": ["A", "B", "C", "D", "E", "F", "G"], "index": {"mean": 3.5, "stddev": 1}}""",
      new Tester.String.Dist("A" -> 61, "B" -> 626, "C" -> 2409, "D" -> 3857, "E" -> 2406, "F" -> 595, "G" -> 46)
    )

    Tester.String("should concatenate results")(
      """{"sumof": ["ABC", "BCD", "CDE"]}""" -> Seq.fill(10)("ABCBCDCDE"),
      """{"sumof": [{"faker": "expression"}, ":::", {"faker": "expression"}]}""" ->
        Seq("Y929:::P626", "V665:::H777", "B539:::E540", "C035:::X869", "C267:::B240", "O691:::L143", "M262:::S632")
    )
  }

  describe("Generate STRING with the faker expression strategy") {
    val expected = Seq("Y929", "V665", "B539", "C035", "C267", "O691", "M262", "O293", "K084", "M371")

    Tester.String("should generate with the defaults when the strategy is selected")(
      """{"faker": "expression"}""" -> expected,
      """{"expression": "#{examplify 'A999'}"}""" -> expected,
      """{"expression": "#{examplify 'A999'}", "length": 10}""" -> expected,
      """{"faker": "expression", "expression": "#{examplify 'A999'}", "length": 10}""" -> expected
    )

    Tester.String("should generate expressions")(
      """{"expression": "#{Name.first_name} #{Name.last_name}"}""" -> Seq(
        "Kit Graham",
        "Dessie McDermott",
        "Carola Runolfsson"
      ),
      """{"expression": "#{Address.street_address}\n#{Address.city}, #{Address.country}"}""" -> Seq(
        "95986 Langworth Bypass\nCarolahaven, Romania",
        "996 Thi Circle\nEast Jeanfort, New Caledonia",
        "1324 O'Reilly Lane\nBernardville, Sweden"
      )
    )
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
