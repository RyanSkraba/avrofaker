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
    val gen = AvroFaker(applyProps(schema, props: _*), new Random(0L))
    LazyList.continually(gen()).flatMap {
      case good: T => Some(good)
      case bad     =>
        // This will fail here
        bad shouldBe a[T]
        None
    }
  }

  def generate[T](schema: String, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] = {
    generate[T](new Schema.Parser().parse(schema), props: _*)
  }

  def generate[T](sType: Schema.Type, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] =
    generate(Schema.create(sType), props: _*)(ct)

  val IntSequence: Schema = applyProps(Schema.create(Schema.Type.INT), ArgStep -> 1)

  describe("Generate INT with the random strategy") {
    val RandomsDefault = Seq(-1630935619, -1483802595, -864264928)
    val RandomsNonNeg = Seq(711764125, 1302116448, 663681053)
    val RandomsByte = Seq(187, 212, 61, 155, 163, 79, 140, 29, 152, 200)

    it("should generate a random Int on unannotated INT schemas") {
      generate[Int](""""int"""").take(3) shouldBe RandomsDefault
      generate[Int]("""{"type": "int"}""").take(3) shouldBe RandomsDefault
    }
    it("should generate a random Int when the random strategy is set") {
      generate[Int]("""{"type": "int", "random": true}""").take(3) shouldBe RandomsDefault
    }
    it("should allow configuring the random strategy with a minimum") {
      generate[Int]("""{"type": "int", "random": {"min": 0}}""").take(3) shouldBe RandomsNonNeg
    }
    it("should implicitly configure the random strategy from the schema annotations") {
      generate[Int]("""{"type": "int", "min": 0}""").take(3) shouldBe RandomsNonNeg
    }
    it("should explicitly choose the random strategy with arguments from the schema annotations") {
      generate[Int]("""{"type": "int", "min": 0, "random": true}""").take(3) shouldBe RandomsNonNeg
    }
    it("should override arguments from the schema annotations") {
      generate[Int]("""{"type": "int", "min": 100, "random": {"min": 0}}""").take(3) shouldBe RandomsNonNeg
    }
    it("should be configurable by the min and the max") {
      generate[Int]("""{"type": "int", "min": 0, "max": 256}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "random": {"min": 0, "max": 256}}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "min": 0, "random": {"max": 256}}""").take(10) shouldBe RandomsByte
      generate[Int]("""{"type": "int", "min": 100, "random": {"min": 0, "max": 256}}""").take(10) shouldBe RandomsByte
    }
    it("should generate random values even if the random strategy is set to false but there is no other strategy") {
      generate[Int]("""{"type": "int", "random": false}""").take(3) shouldBe RandomsDefault
    }
  }

  describe("Generate INT with the gauss strategy") {
    val GaussDefault = Seq(80, -90, 208, 76, 98, -168, -2, 11, -39, -64)

    it(
      "should generate between a bell curve of numbers centered around 0, with 95% of the values between -200 and 200"
    ) {
      // These are all equivalent
      generate[Int]("""{"type": "int", "gauss": true}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "random" : false, "gauss": true}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : 0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : 0.0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : "0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean" : "0.0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : 100}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : 100.0}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "stddev" : "100.0"}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "gauss": {}}""").take(10) shouldBe GaussDefault
      generate[Int]("""{"type": "int", "mean": 0, "gauss": {"stddev": 100}}""").take(10) shouldBe GaussDefault
    }

    it(
      "should generate numbers centered around 100, with 95% of the values between `80` and `120`, with possible negatives"
    ) {
      // Note that it is *extremely* unlikely that a value outside 10 standard deviations would ever occur.
      generate[Int]("""{"type": "int", "gauss": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Seq(108, 90, 120,
        107, 109, 83, 99, 101, 96, 93)
    }
    it(
      "should generate numbers centered around 100, with 95% of the values between `80` and `120`, guaranteed no negatives"
    ) {
      // But here it is impossible
      generate[Int]("""{"type": "int", "min": 0, "gauss": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Seq(108,
        90, 120, 107, 109, 83, 99, 101, 96, 93)
    }
    it("should apply a filter to only return numbers greater than the mean") {
      generate[Int]("""{"type": "int", "min": 100, "gauss": {"mean": 100, "stddev": 10}}""").take(10) shouldBe Seq(108,
        120, 107, 109, 101, 100, 105, 102, 114, 102)
      // Be careful about setting a valid interval that is unlikely to have a generated number.  This next example
      // requires generated values to be 5 standard deviations from the main, so only one out of every million
      // generated values will be retained.
      // generate[Int]("""{"type": "int", "max": 50, "gauss": {"mean": 100, "stddev": 10}}""").take(10)
    }
  }

  describe("Examples from the readme.md") {

    /*

| Schema                                                  | Summary                                                                                                         |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
generate[Int]("""{"type": "int", "sequence": true}`                     generate[Int]("""0`, `1`, `2`, `3`, `4`, `5` until it reaches `2147483646`, then restarts.                                      |
generate[Int]("""{"type": "int", "step": 1}`                            | :arrow_up: Equivalent, implicitly choosing the **sequence** strategy.                                           |
generate[Int]("""{"type": "int", "start": 10000}`                       | :arrow_up: `10000`, `10001`, `10002`, `10003`, `10004`, `10005`, implicitly choosing the **sequence** strategy. |
generate[Int]("""{"type": "int", "step": -1, "max": 4}`                 | `3`, `2`, `1`, `0`, `3`, `2`, etc.                                                                              |
generate[Int]("""{"type": "int", "start": 3, "max": 4, "step": 3}`      | :arrow_up: Equivalent because of the wrapped remainder, but confusing.                                          |
generate[Int]("""{"type": "int", "sequence": {"start": 10, "max": 13}}` | `10`, `11`, `12`, `0`, `1`, `2`, etc.                                                                           |
generate[Int]("""{"type": "int", "sequence": {"start": 10, "max": 13}}` | `10`, `11`, `12`, `0`, `1`, `2`, etc.                                                                           |
generate[Int]("""{"type": "int", "max": 13, "sequence": {"start": 10}}` | :arrow_up: Equivalent, inheriting the `max` argument from the parent.                                           |
     */
    it("should work from the INT Sequences section") {}

    /*

| Schema                                                               | Summary                                                                                                                              |
|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
generate[Int]("""{"type": "int", "value": 123}`                                      | Always generates `123`.                                                                                                              |
generate[Int]("""{"type": "int", "value": {"value": 123}}`                           | :arrow_up: Equivalent, but useless.                                                                                                  |
generate[Int]("""{"type": "int", "value": [123, 321]}`                               | Randomly picks `123` or `321`.                                                                                                       |
generate[Int]("""{"type": "int", "value": {"random": {"min": 0}}}`                   | **random** generating a  whole number from 0 to **2147483646**.                                                                      |
generate[Int]("""{"type": "int", "max": 4, "value": {"start": 1, "sequence": true}}` | **sequence** generating `1`, `2`, `3`, `0`, `1`, `2`, etc.   Note that the sequence arguments are inherited up to the parent schema. |
generate[Int]("""{"type": "int", "value": [999, {"random": {"min": 0, "max": 9}}]}`  | Picks `999` 50% of the time, and a single digit number the other 50%.                                                                |

If it is a JSON array, and at the _same_ level as the `value` attribute, there exists an `index` attribute that _is_ a string, then

1. The value of the `index` attribute is used to generate an integer value within the interval [`0`, array size).
 The process is identical to how a `value` attribute is processed (as a constant, a generator strategy, or an array of generator strategies),
 with an implicit `min` and `max` supplied to the strategies.
 If the result happens to be outside the interval, either `min` or `max` is used instead.
2. The generated integer is used to pick the value in the `value` JSON array that is actually used for the generator.

| Schema                                                                                | Summary                                                                                       |
|---------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
generate[Int]("""{"type": "int", "value": [123, 321, 999], "index": 0}`                               | Always generates `123`.                                                                       |
generate[Int]("""{"type": "int", "value": [123, 321, 999], "index": -1}`                              | :arrow_up: Equivalent.                                                                        |
generate[Int]("""{"type": "int", "value": [123, 321, 999], "index": {"step": 1}}`                     | The index is a sequence `0`,`1`,`2`,`0`,`1`, etc, so alternates `123`,`321`,`999`,`123`, etc. |
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": {"gauss": {"mean": 2, "stddev": 1}}}` | Picks the numbers `1`,`2`,`3`,`4`,`5` in a rough bell curve.                                  |


| Schema                                                      | Summary                 |
|-------------------------------------------------------------|-------------------------|
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": "sum"}`     | Always generates `15`.  |
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": "product"}` | Always generates `120`. |
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": "min"}`     | Always generates `1`.   |
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": "max"}`     | Always generates `5`.   |
generate[Int]("""{"type": "int", "value": [1,2,3,4,5], "index": "mean"}`    | Always generates `3`.   |

     */

    it("should work from the INT Value section ") {}

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
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 0).set("name", "CCzLNHBFHu").build()
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 1).set("name", "RvbI1iI19W").build()
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 2).set("name", "jGGR8UNWut").build()
    }
  }

  describe("Generating Avro ENUM data") {
    it("should pick symbols randomly") {
      val schema = SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E")
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe "A"
      gen.apply() shouldBe "D"
      gen.apply() shouldBe "E"
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
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe Array("CzLNHBFHuR", "vbI1iI19Wj")
      gen.apply() shouldBe Array("GR8UNWutFR", "ZvWebpA5WH")
      gen.apply() shouldBe Array("yqts0coJXQ", "qPyuxbr589", "wyJzS2SuiH", "rAOB2RuvBb")
    }
  }

  describe("Generating Avro MAP data") {
    it("should create maps of its value type") {
      val schema = SchemaBuilder.map().values().`type`(IntSequence)
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe Map("CzLNHBFHuR" -> 0, "vbI1iI19Wj" -> 1)
      gen.apply() shouldBe Map("GR8UNWutFR" -> 2, "ZvWebpA5WH" -> 3)
      gen.apply() shouldBe Map("yqts0coJXQ" -> 4, "qPyuxbr589" -> 5, "wyJzS2SuiH" -> 6, "rAOB2RuvBb" -> 7)
    }
  }

  describe("Generating Avro UNION data") {
    it("should generate data from the union types with equal probability") {
      val gen = AvroFaker(Schema.createUnion(Schema.create(Schema.Type.NULL), IntSequence), new Random(0L))
      gen() shouldBe 0
      gen() shouldBe 1
      Option(gen()) shouldBe None
      gen() shouldBe 2
      gen() shouldBe 3
      Option(gen()) shouldBe None
      gen() shouldBe 4
      Option(gen()) shouldBe None
      gen() shouldBe 5
    }
  }

  describe("Generating Avro FIXED data") {
    it("should create byte arrays by default") {
      val gen = AvroFaker(Schema.createFixed("Example", "", "", 4), new Random(0L))
      gen() shouldBe Array(96, -76, 32, -69)
      gen() shouldBe Array(56, 81, -39, -44)
      gen() shouldBe Array(122, -53, -109, 61)
    }
  }

  describe("Generating Avro STRING data") {
    it("should create ten character strings by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.STRING), new Random(0L))
      gen() shouldBe "CCzLNHBFHu"
      gen() shouldBe "RvbI1iI19W"
      gen() shouldBe "jGGR8UNWut"
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
      val gen = AvroFaker(Schema.create(Schema.Type.BYTES), new Random(0L))
      gen() shouldBe Array(56, 81, -39, -44, 122)
      gen() shouldBe Array(-10, -55, 45, -93, 58, -16, 29)
      gen() shouldBe Array(3, 37, -12, 29, 62, -70)
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
        generate(schemaType, ArgMax -> 0.5, ArgStep -> -0.25)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(0.5,
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
      val gen = AvroFaker(Schema.create(Schema.Type.BOOLEAN), new Random(0L))
      gen() shouldBe true
      gen() shouldBe true
      gen() shouldBe false
    }
  }

  describe("Generating Avro NULL data") {
    it("should only ever generate NULL") {
      val gen = AvroFaker(Schema.create(Schema.Type.NULL), new Random(0L))
      Option(gen()) shouldBe None
      Option(gen()) shouldBe None
      Option(gen()) shouldBe None
    }
  }
}
