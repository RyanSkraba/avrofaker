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

  /** Create an AvroFaker from the given schema with the given properties.
    * @param sType
    *   The base schema type to use
    * @param props
    *   Pairs of property keys and values to apply to the schema
    * @tparam T
    *   The expected type of the contents being generated
    * @return
    *   A LazyList stream of fake values.
    */
  def generate[T](sType: Schema.Type, props: (String, Any)*)(implicit ct: ClassTag[T]): LazyList[T] =
    generate(Schema.create(sType), props: _*)(ct)

  val IntSequence: Schema = applyProps(Schema.create(Schema.Type.INT), PropStart -> 0)

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
      generate[String](SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E"), PropStart -> 0)
        .take(10) shouldBe Seq("A", "B", "C", "D", "E", "A", "B", "C", "D", "E")
    }

    it("should pick symbols sequentially with a step") {
      generate[String](
        SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E"),
        PropStart -> 0,
        PropStep -> 3
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
        generate(schemaType, PropMin -> -1, PropMax -> 2)(ct).take(10) shouldBe Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
      }

      it("should start at the specified value") {
        generate(schemaType, PropStart -> 10)(ct).take(10) shouldBe Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
      }

      it("should rotate before reaching the end") {
        generate(schemaType, PropStart -> 1, PropEnd -> 4)(ct).take(10) shouldBe Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
      }

      it("should have a configurable step before reaching the end") {
        generate(schemaType, PropStart -> -5, PropEnd -> 5, PropStep -> 3)(ct).take(15) shouldBe Seq(-5, -2, 1, 4, -3,
          0, 3, -4, -1, 2, -5, -2, 1, 4, -3)
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
        val gen = generate(schemaType, AvroFaker.PropMin -> 10, AvroFaker.PropMax -> 20)(ct).map(_.toString.toDouble)
        gen.head shouldBe 17.30967787376657 +- precision
        gen(1) shouldBe 12.405364156714858 +- precision
        gen(2) shouldBe 16.37417425350108 +- precision
      }

      it("should generate guassian distribution") {
        val gen = generate(schemaType, AvroFaker.PropMean -> 0.0)(ct).map(_.toString.toDouble)
        gen.head shouldBe 0.8025330637390305 +- precision
        gen(1) shouldBe -0.9015460884175122 +- precision
        gen(2) shouldBe 2.080920790428163 +- precision
      }

      it("should start at the specified value") {
        generate(schemaType, PropStart -> 10)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(10d, 11d, 12d, 13d,
          14d, 15d, 16d, 17d, 18d, 19d)
      }

      it("should rotate before reaching the end") {
        generate(schemaType, PropStart -> 1, PropEnd -> 4)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(1d, 2d,
          3d, 1d, 2d, 3d, 1d, 2d, 3d, 1d)
      }

      it("should start at the specified value at fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(schemaType, PropStart -> 0.5, PropStep -> -0.25)(ct).map(_.toString.toDouble).take(10) shouldBe Seq(
          0.5, 0.25, 0.0, -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75)
      }

      it("should rotate before reaching the end with fractional values") {
        // Note that these values are all perfectly representable in floating point
        generate(schemaType, PropStart -> 0.75, PropEnd -> 2.5, PropStep -> 0.5)(ct)
          .map(_.toString.toDouble)
          .take(10) shouldBe Seq(0.75, 1.25, 1.75, 2.25, 1.0, 1.5, 2.0, 0.75, 1.25, 1.75)
      }

      it("should have a configurable step before reaching the end") {
        generate(schemaType, PropStart -> -5, PropEnd -> 5, PropStep -> 3)(ct)
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
