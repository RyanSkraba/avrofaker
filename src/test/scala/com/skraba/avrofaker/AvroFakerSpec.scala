package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag
import scala.util.Random

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

  /** Create an AvroFaker from the given schema with the given properties.
    * @param schema
    *   The base schema to use
    * @param props
    *   Pairs of property keys and values to apply to the schema
    * @tparam T
    *   The expected type of the contents being generated
    * @return
    *   A LazyList stream of fake values.
    */
  def generate[T](schema: Schema, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] = {
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

  def generate[T](schema: String, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] = {
    generate[T](new Schema.Parser().parse(schema), props: _*)(ct)
  }

  def generate[T](sType: Schema.Type, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] =
    generate(Schema.create(sType), props: _*)(ct)

  val IntSequence: Schema = applyProps(Schema.create(Schema.Type.INT), ArgStep -> 1)

  describe("Generate INT with the random strategy") {
    val RandomsDefault = Seq(-1630935619, -1483802595, -864264928)
    val RandomsNonNeg = Seq(711764125, 1302116448, 663681053)
    val RandomsByte = Seq(187, 212, 61, 155, 163, 79, 140, 29, 152, 200)

    it("should use the random strategy on unannotated INT schemas") {
      generate[Int](""""int"""").take(3) shouldBe RandomsDefault
      generate[Int]("""{"type": "int"}""").take(3) shouldBe RandomsDefault
    }

    it("should generate a random Int when the random strategy is explicitly set") {
      generate[Int]("""{"type": "int", "faker": "random"}""").take(3) shouldBe RandomsDefault
      generate[Int]("""{"type": "int", "faker": {"faker": "random"}}""").take(3) shouldBe RandomsDefault
    }

    it("should generate a random Int when the random strategy is unset") {
      generate[Int]("""{"type": "int", "faker": {}}""").take(3) shouldBe RandomsDefault
    }

    it("should allow configuring the random strategy with a minimum") {
      generate[Int]("""{"type": "int", "min": 0, "faker": "random"}""").take(3) shouldBe RandomsNonNeg
      generate[Int]("""{"type": "int", "faker": {"min": 0}}""").take(3) shouldBe RandomsNonNeg
    }

    it("should implicitly configure the random strategy from the schema annotations") {
      generate[Int]("""{"type": "int", "min": 0}""").take(3) shouldBe RandomsNonNeg
    }

    it("should override arguments from the schema annotations") {
      generate[Int]("""{"type": "int", "min": 100, "faker": {"min": 0}}""").take(3) shouldBe RandomsNonNeg
    }

    it("should be configurable by the min and the max") {
      generate[Int]("""{"type": "int", "min": 0, "max": 256}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "faker": {"min": 0, "max": 256}}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "min": 0, "faker": {"max": 256}}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "min": 100, "faker": {"min": 0, "max": 256}}""").take(10) shouldBe RandomsByte
    }

    for (maxValue <- Seq("256", "256.1", "256.999"))
      it(s"should support the max value: $maxValue and \"$maxValue\"") {
        generate[Int](s"""{"type": "int", "min": 0, "max": $maxValue}""").take(10) shouldBe RandomsByte
        generate[Int](s"""{"type": "int", "min": 0, "max": "$maxValue"}""").take(10) shouldBe RandomsByte
      }
  }

  describe("Generate INT with the gauss strategy") {
    val GaussDefault = Seq(80, -90, 208, 76, 98, -168, -2, 11, -39, -64)
    val Gauss100_10 = Seq(108, 90, 120, 107, 109, 83, 99, 101, 96, 93)

    it(
      "should generate between a bell curve of numbers centered around 0, with 95% of the values between -200 and 200"
    ) {
      // These are all equivalent
      generate[Int]("""{"type": "int", "faker": "gauss"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : 0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : 0.0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : "0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : "0.0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : 100}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : "100"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : 100.0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : "100.0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean": 0, "faker": {"stddev": 100}}""").take(10) shouldBe GaussDefault
    }
    it(
      "should generate numbers centered around 100, with 95% of the values between `80` and `120`, with possible negatives"
    ) {
      // Note that it is *extremely* unlikely that a value outside 10 standard deviations would ever occur.
      generate[Int]("""{"type": "int", "mean": 100, "stddev": 10}""").take(10) shouldBe Gauss100_10
      generate[Int]("""{"type": "int", "mean": 100, "faker": {"stddev": 10}}""").take(10) shouldBe Gauss100_10
      generate[Int]("""{"type": "int", "stddev": 10, "faker": {"mean": 100}}""").take(10) shouldBe Gauss100_10
      generate[Int]("""{"type": "int", "faker": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Gauss100_10
    }
    it(
      "should generate numbers centered around 100, with 95% of the values between `80` and `120`, guaranteed no negatives"
    ) {
      // But here it is impossible
      generate[Int]("""{"type": "int", "min": 0, "faker": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Gauss100_10
    }
    it("should apply a filter to only return numbers greater than the mean") {
      generate[Int]("""{"type": "int", "min": 100, "faker": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Seq(108,
        120, 107, 109, 101, 100, 105, 102, 114, 102)
      // Be careful about setting a valid interval that is unlikely to have a generated number.  This next example
      // requires generated values to be 5 standard deviations from the main, so only one out of every million
      // generated values will be retained.
      // generate[Int]("""{"type": "int", "max": 50, "gauss": {"mean": 100, "stddev": 10}}""").take(10)
    }
  }

  describe("Generate INT with the sequence strategy") {
    it("should generate a simple sequence") {
      generate[Int]("""{"type": "int", "faker": "sequence"}""").take(10) shouldBe (0 to 9)
      generate[Int]("""{"type": "int", "step": 1}""").take(10) shouldBe (0 to 9)
    }
    it("should start from a specific value") {
      generate[Int]("""{"type": "int", "start": 10000}""").take(10) shouldBe (10000 to 10009)
    }
    it("should start from a specific value and step") {
      generate[Int]("""{"type": "int", "start": 10, "step": 2}""").take(10) shouldBe (10 to 28 by 2)
    }
    it("should count down with a negative step") {
      generate[Int]("""{"type": "int", "step": -1, "max": 4}""").take(10) shouldBe Seq(3, 2, 1, 0, 3, 2, 1, 0, 3, 2)
    }
    it("should look like a countdown down when you loop every step") {
      generate[Int]("""{"type": "int", "start": 3, "max": 4, "step": 3}""").take(6) shouldBe Seq(3, 2, 1, 0, 3, 2)
    }
    it("should start from a specific value and loop") {
      generate[Int]("""{"type": "int", "faker": {"start": 10, "max": 13}}""").take(5) shouldBe Seq(10, 11, 12, 0, 1)
      generate[Int]("""{"type": "int", "max": 13, "faker": {"start": 10}}""").take(5) shouldBe Seq(10, 11, 12, 0, 1)
    }
  }

  describe("Generate INT with the value strategy") {
    it("should generate a constant value") {
      // These are all equivalent
      generate[Int]("""{"type": "int", "value": 123}""").take(10) shouldBe Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "faker": 123}""").take(10) shouldBe Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "faker": "value", "value": 123}""").take(10) shouldBe Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "value": 123.9}""").take(10) shouldBe Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "value": "123.1"}""").take(10) shouldBe Seq.fill(10)(123)
    }

    it("should apply the minimum and maximum to the constant value") {
      generate[Int]("""{"type": "int", "min": 0, "value": 123}""").take(10) shouldBe Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "min": 234, "value": 123}""").take(10) shouldBe Seq.fill(10)(234)
      generate[Int]("""{"type": "int", "max": 0, "value": 123}""").take(10) shouldBe Seq.fill(10)(0)
      generate[Int]("""{"type": "int", "max": 234, "value": 123}""").take(10) shouldBe Seq.fill(10)(123)
    }

    for (maxValue <- Seq("256", "256.1", "256.999"))
      it(s"should support the max and min value: $maxValue and \"$maxValue\"") {
        generate[Int](s"""{"type": "int", "value": 999, "max": $maxValue}""").take(10) shouldBe Seq.fill(10)(256)
        generate[Int](s"""{"type": "int", "value": 999, "max": "$maxValue"}""").take(10) shouldBe Seq.fill(10)(256)
        generate[Int](s"""{"type": "int", "value": 123, "min": $maxValue}""").take(10) shouldBe Seq.fill(10)(256)
        generate[Int](s"""{"type": "int", "value": 123, "min": "$maxValue"}""").take(10) shouldBe Seq.fill(10)(256)
      }
  }

  describe("Generate INT with the oneof strategy") {
    it("should pick a random value between 123, 234 and 345") {
      // These are all equivalent
      val expected = Seq(123, 234, 234, 345, 345, 345, 345, 123, 123, 345)
      generate[Int]("""{"type": "int", "faker": [123, 234, 345]}""").take(10) shouldBe expected
      generate[Int]("""{"type": "int", "oneof": [123, 234, 345]}""").take(10) shouldBe expected
    }

    it("should pick randomly between a single digit and 999") {
      generate[Int]("""{"type": "int", "faker": [999, {"min": 0, "max": 9}]}""").take(10) shouldBe Seq(7, 999, 8, 999,
        3, 2, 999, 999, 999, 8)
    }

    it("should always generate 123") {
      val expected = Seq.fill(10)(123)
      generate[Int]("""{"type": "int", "oneof": [123, 321, 999], "index": 0}""").take(10) shouldBe expected
      generate[Int]("""{"type": "int", "oneof": 123}""").take(10) shouldBe expected
      generate[Int]("""{"type": "int", "oneof": [123, 321, 999], "index": -1}""").take(10) shouldBe expected
    }

    it("should cycle through the elements") {
      generate[Int]("""{"type": "int", "oneof": [123, 321, 999], "index": {"step": 1}}""").take(10) shouldBe
        Seq(123, 321, 999, 123, 321, 999, 123, 321, 999, 123)
    }

    it("should pick a gaussian distribution of elements") {
      generate[Int]("""{"type": "int", "oneof": [1,2,3,4,5], "index": {"mean": 2.5, "stddev": 1}}""")
        .take(1000)
        .groupBy(identity)
        .view
        .mapValues(_.size)
        .toMap shouldBe Map(1 -> 82, 2 -> 209, 3 -> 392, 4 -> 255, 5 -> 62)
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
      generate[String](SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E"), ArgStep -> 1)
        .take(10) shouldBe Seq("A", "B", "C", "D", "E", "A", "B", "C", "D", "E")
    }

    it("should pick symbols sequentially with a step") {
      generate[String](
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
      generate[String](Schema.Type.STRING, PropLength -> 5)
        .take(5) shouldBe Seq("CCzLN", "HBFHu", "RvbI1", "iI19W", "jGGR8")
    }

    /** Generate a list from a faker expression. */
    def genFakeString(expression: String): LazyList[String] =
      generate[String](Schema.Type.STRING, PropFaker -> expression)

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
  for ((schemaType, ct) <- Seq(Schema.Type.INT -> ClassTag.Int, Schema.Type.LONG -> ClassTag.Long))
    describe(s"Generating Avro $schemaType data (common)") {
      it("should be random by default") {
        generate(schemaType, ArgMin -> -1, ArgMax -> 2)(ct).take(10) shouldBe Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
      }

      it("should start at the specified value") {
        generate(schemaType, ArgMin -> 10, ArgStep -> 1)(ct).take(10) shouldBe Seq(10, 11, 12, 13, 14, 15, 16, 17, 18,
          19)
      }

      it("should rotate before reaching the end") {
        generate(schemaType, ArgMin -> 1, ArgMax -> 4, ArgStep -> 1)(ct).take(10) shouldBe Seq(1, 2, 3, 1, 2, 3, 1, 2,
          3, 1)
      }

      it("should have a configurable step before reaching the end") {
        generate(schemaType, ArgMin -> -5, ArgMax -> 5, ArgStep -> 3)(ct).take(15) shouldBe Seq(-5, -2, 1, 4, -3, 0, 3,
          -4, -1, 2, -5, -2, 1, 4, -3)
      }
    }

  describe("Generating Avro LONG data") {
    it("should generate random numbers") {
      generate[Long](Schema.Type.LONG)
        .take(4) shouldBe Seq(-4962768465676381896L, 4437113781045784766L, -6688467811848818630L, -8292973307042192125L)
    }
  }

  describe("Generating Avro INT data") {
    it("should generate random numbers") {
      generate[Int](Schema.Type.INT).take(4) shouldBe Seq(-1630935619, -1483802595, -864264928, -530909147)
    }
  }

  // DOUBLE and FLOAT schema types are identical except for the precision.  The type is checked in the generate method, so we force it to a Double for the value check
  for (
    (schemaType, ct, precision) <- Seq(
      (Schema.Type.FLOAT, ClassTag.Float, 1e-6),
      (Schema.Type.DOUBLE, ClassTag.Double, 1e-14)
    )
  ) {
    describe(s"Generating Avro $schemaType data (common)") {
      it("should generate random numbers") {
        val gen = generate(schemaType)(ct).map(_.toString.toDouble)
        gen.head shouldBe 0.730967787376657 +- precision
        gen(1) shouldBe 0.24053641567148587 +- precision
        gen(2) shouldBe 0.6374174253501083 +- precision
      }

      it("should generate random numbers with a minimum and maximum") {
        val gen = generate(schemaType, AvroFaker.ArgMin -> 10, AvroFaker.ArgMax -> 20)(ct).map(_.toString.toDouble)
        gen.head shouldBe 17.30967787376657 +- precision
        gen(1) shouldBe 12.405364156714858 +- precision
        gen(2) shouldBe 16.37417425350108 +- precision
      }

      it("should generate guassian distribution") {
        val gen = generate(schemaType, AvroFaker.ArgMean -> 0.0)(ct).map(_.toString.toDouble)
        gen.head shouldBe 0.8025330637390305 +- precision
        gen(1) shouldBe -0.9015460884175122 +- precision
        gen(2) shouldBe 2.080920790428163 +- precision
      }

      it("should start at the specified value") {
        generate(schemaType, ArgMin -> 10, ArgStep -> 1)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(10d, 11d,
          12d, 13d, 14d, 15d, 16d, 17d, 18d, 19d)
      }

      it("should rotate before reaching the end") {
        generate(schemaType, ArgMin -> 1, ArgMax -> 4, ArgStep -> 1)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(
          1d, 2d, 3d, 1d, 2d, 3d, 1d, 2d, 3d, 1d)
      }

      it("should start at the specified value at fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(schemaType, ArgMax -> 0.75, ArgStep -> -0.25)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(0.5,
          0.25, 0.0, -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75)
      }

      it("should rotate before reaching the end with fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(schemaType, ArgMin -> 0.75, ArgMax -> 2.5, ArgStep -> 0.5)(ct)
          .map(_.toString.toDouble)
          .take(10) shouldBe Seq(0.75, 1.25, 1.75, 2.25, 1.0, 1.5, 2.0, 0.75, 1.25, 1.75)
      }

      it("should have a configurable step before reaching the end") {
        generate(schemaType, ArgMin -> -5, ArgMax -> 5, ArgStep -> 3)(ct)
          .map(_.toString.toDouble)
          .take(15) shouldBe Seq(-5d, -2d, 1d, 4d, -3d, 0d, 3d, -4d, -1d, 2d, -5d, -2d, 1d, 4d, -3d)
      }
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
