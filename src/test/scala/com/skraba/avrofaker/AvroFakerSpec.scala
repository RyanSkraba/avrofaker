package com.skraba.avrofaker

import com.skraba.avrofaker.AvroFaker._
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class AvroFakerSpec extends AnyFunSpecLike with Matchers {
  // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL

  val IntSequence: Schema = {
    val schema = Schema.create(Schema.Type.INT)
    schema.addProp(PropStart, 0)
    schema
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
        .endRecord();
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 0).set("name", "CCzLNHBFHu").build()
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 1).set("name", "RvbI1iI19W").build()
      gen.apply() shouldBe new GenericRecordBuilder(schema).set("id", 2).set("name", "jGGR8UNWut").build()
    }
  }

  describe("Generating Avro ENUM data") {
    it("should pick symbols randomly") {
      val schema = SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E");
      val gen = AvroFaker(schema, new Random(0L))
      gen.apply() shouldBe "A"
      gen.apply() shouldBe "D"
      gen.apply() shouldBe "E"
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

    /** Generate a list from a faker expression. */
    def genFakeString(expression: String): LazyList[Any] = {
      val schema = Schema.create(Schema.Type.STRING)
      schema.addProp(PropFaker, expression)
      val gen = AvroFaker(schema, new Random(0L))
      LazyList.continually(gen())
    }

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
  for (schemaType <- Seq(Schema.Type.INT, Schema.Type.LONG))
    describe(s"Generating Avro $schemaType data (common)") {

      def gen10(schema: Schema): Seq[Any] = {
        val generator = AvroFaker(schema, new Random(0L))
        val values = LazyList.continually(generator()).take(10)
        values.head shouldBe (if (schemaType == Schema.Type.INT) a[Int] else a[Long])
        values
      }

      it("should be random by default") {
        val schema = Schema.create(schemaType)
        schema.addProp(PropMin, "-1")
        schema.addProp(PropMax, "2")
        gen10(schema) shouldBe Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
      }

      it("should start at the specified value") {
        val schema = Schema.create(schemaType)
        schema.addProp(PropStart, "10")
        gen10(schema) shouldBe Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
      }

      it("should rotate before reaching the end") {
        val schema = Schema.create(schemaType)
        schema.addProp(PropStart, "1")
        schema.addProp(PropEnd, "4")
        gen10(schema) shouldBe Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
      }

      it("should have a configurable step before reaching the end") {
        val schema = Schema.create(schemaType)
        schema.addProp(PropStart, "-10")
        schema.addProp(PropEnd, "10")
        schema.addProp(PropStep, "3")
        gen10(schema) shouldBe Seq(-10, -7, -4, -1, 2, 5, 8, -10, -7, -4)
      }
    }

  describe("Generating Avro LONG data") {
    it("should generate random numbers") {
      val schema = Schema.create(Schema.Type.LONG)
      val gen = AvroFaker(schema, new Random(0L))
      val values = LazyList.continually(gen()).take(4)
      values.head shouldBe a[Long]
      values shouldBe Seq(-4962768465676381896L, 4437113781045784766L, -6688467811848818630L, -8292973307042192125L)
    }
  }

  describe("Generating Avro INT data") {
    it("should generate random numbers") {
      val schema = Schema.create(Schema.Type.INT)
      val gen = AvroFaker(schema, new Random(0L))
      val values = LazyList.continually(gen()).take(4)
      values.head shouldBe a[Int]
      values shouldBe Seq(-1630935619, -1483802595, -864264928, -530909147)
    }
  }

  describe("Generating Avro FLOAT data") {
    it("should generate random numbers") {
      val gen = AvroFaker(Schema.create(Schema.Type.FLOAT), new Random(0L))
      gen().asInstanceOf[Float] shouldBe 0.730967787376657f +- 1e-7f
      gen().asInstanceOf[Float] shouldBe 0.24053641567148587f +- 1e-7f
      gen().asInstanceOf[Float] shouldBe 0.6374174253501083f +- 1e-7f
    }
  }

  describe("Generating Avro DOUBLE data") {
    it("should generate random numbers") {
      val gen = AvroFaker(Schema.create(Schema.Type.DOUBLE), new Random(0L))
      gen().toString.toDouble shouldBe 0.730967787376657 +- 1e-14
      gen().toString.toDouble shouldBe 0.24053641567148587 +- 1e-14
      gen().toString.toDouble shouldBe 0.6374174253501083 +- 1e-14
    }
  }

  describe("Generating Avro BOOLEAN data") {
    it("should generate random booelans") {
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
