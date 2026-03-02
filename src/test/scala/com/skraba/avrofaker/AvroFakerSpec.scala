package com.skraba.avrofaker

import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class AvroFakerSpec extends AnyFunSpecLike with Matchers {
  // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL

  describe("Generating Avro RECORD data") {
    it("should create ten character strings by default") {
      val schema = SchemaBuilder.record("Example").fields().requiredInt("id").requiredString("name").endRecord();
      val gen = AvroFaker(schema, new Random(0L))
      gen.generate() shouldBe new GenericRecordBuilder(schema).set("id", 0).set("name", "CCzLNHBFHu").build()
      gen.generate() shouldBe new GenericRecordBuilder(schema).set("id", 1).set("name", "RvbI1iI19W").build()
      gen.generate() shouldBe new GenericRecordBuilder(schema).set("id", 2).set("name", "jGGR8UNWut").build()
    }
  }

  describe("Generating Avro ENUM data") {
    it("should pick symbols randomly") {
      val schema = SchemaBuilder.enumeration("Example").symbols("A", "B", "C", "D", "E");
      val gen = AvroFaker(schema, new Random(0L))
      gen.generate() shouldBe "A"
      gen.generate() shouldBe "D"
      gen.generate() shouldBe "E"
    }
  }

  describe("Generating Avro ARRAY data") {
    it("should create arrays of its element type") {
      val schema = SchemaBuilder.array().items().stringBuilder().endString()
      val gen = AvroFaker(schema, new Random(0L))
      gen.generate() shouldBe Array("CzLNHBFHuR", "vbI1iI19Wj")
      gen.generate() shouldBe Array("GR8UNWutFR", "ZvWebpA5WH")
      gen.generate() shouldBe Array("yqts0coJXQ", "qPyuxbr589", "wyJzS2SuiH", "rAOB2RuvBb")
    }
  }

  describe("Generating Avro MAP data") {
    it("should create maps of its value type") {
      val schema = SchemaBuilder.map().values().intBuilder().endInt()
      val gen = AvroFaker(schema, new Random(0L))
      gen.generate() shouldBe Map("CzLNHBFHuR" -> 0, "vbI1iI19Wj" -> 1)
      gen.generate() shouldBe Map("GR8UNWutFR" -> 2, "ZvWebpA5WH" -> 3)
      gen.generate() shouldBe Map("yqts0coJXQ" -> 4, "qPyuxbr589" -> 5, "wyJzS2SuiH" -> 6, "rAOB2RuvBb" -> 7)
    }
  }

  describe("Generating Avro UNION data") {
    it("should generate data from the union types with equal probability") {
      val gen = AvroFaker(
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)),
        new Random(0L)
      )
      gen.generate() shouldBe 0
      gen.generate() shouldBe 1
      Option(gen.generate()) shouldBe None
      gen.generate() shouldBe 2
      gen.generate() shouldBe 3
      Option(gen.generate()) shouldBe None
      gen.generate() shouldBe 4
      Option(gen.generate()) shouldBe None
      gen.generate() shouldBe 5
    }
  }

  describe("Generating Avro FIXED data") {
    it("should create byte arrays by default") {
      val gen = AvroFaker(Schema.createFixed("Example", "", "", 4), new Random(0L))
      gen.generate() shouldBe Array(96, -76, 32, -69)
      gen.generate() shouldBe Array(56, 81, -39, -44)
      gen.generate() shouldBe Array(122, -53, -109, 61)
    }
  }

  describe("Generating Avro STRING data") {
    it("should create ten character strings by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.STRING), new Random(0L))
      gen.generate() shouldBe "CCzLNHBFHu"
      gen.generate() shouldBe "RvbI1iI19W"
      gen.generate() shouldBe "jGGR8UNWut"
    }
  }

  describe("Generating Avro BYTES data") {
    it("should create variable byte arrays by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.BYTES), new Random(0L))
      gen.generate() shouldBe Array(56, 81, -39, -44, 122)
      gen.generate() shouldBe Array(-10, -55, 45, -93, 58, -16, 29)
      gen.generate() shouldBe Array(3, 37, -12, 29, 62, -70)
    }
  }

  // LONG and INT schema types are identical except for the return type
  for (schemaType <- Seq(Schema.Type.INT, Schema.Type.LONG))
    describe(s"Generating Avro $schemaType data (common)") {

      def gen10(schema: Schema): Seq[Any] = {
        val generator = AvroFaker(schema, new Random(0L))
        val values = LazyList.continually(generator.generate()).take(10)
        values.head shouldBe (if (schemaType == Schema.Type.INT) a[Int] else a[Long])
        values
      }

      it("should create a sequence by default") {
        gen10(Schema.create(schemaType)) shouldBe Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
      }

      it("should start at the minimum") {
        val schema = Schema.create(schemaType)
        schema.addProp("number.min", "10")
        gen10(schema) shouldBe Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
      }

      it("should rotate before reaching the maximum") {
        val schema = Schema.create(schemaType)
        schema.addProp("number.min", "1")
        schema.addProp("number.max", "4")
        gen10(schema) shouldBe Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
      }

      it("should have a configurable step before reaching the maximum") {
        val schema = Schema.create(schemaType)
        schema.addProp("number.min", "-10")
        schema.addProp("number.max", "10")
        schema.addProp("number.step", "3")
        gen10(schema) shouldBe Seq(-10, -7, -4, -1, 2, 5, 8, -10, -7, -4)
      }

      it("should support the random strategy with min and max") {
        val schema = Schema.create(schemaType)
        schema.addProp("number.min", "-1")
        schema.addProp("number.max", "2")
        schema.addProp("number.strategy", "random")
        gen10(schema) shouldBe Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
      }
    }

  describe("Generating Avro LONG data") {
    it("should generate random numbers") {
      val schema = Schema.create(Schema.Type.LONG)
      schema.addProp("number.strategy", "random")
      val gen = AvroFaker(schema, new Random(0L))
      val values = LazyList.continually(gen.generate()).take(4)
      values.head shouldBe a[Long]
      values shouldBe Seq(-4962768465676381896L, 4437113781045784766L, -6688467811848818630L, -8292973307042192125L)
    }
  }

  describe("Generating Avro INT data") {
    it("should generate random numbers") {
      val schema = Schema.create(Schema.Type.INT)
      schema.addProp("number.strategy", "random")
      val gen = AvroFaker(schema, new Random(0L))
      val values = LazyList.continually(gen.generate()).take(4)
      values.head shouldBe a[Int]
      values shouldBe Seq(-1630935619, -1483802595, -864264928, -530909147)
    }
  }

  describe("Generating Avro FLOAT data") {
    it("should generate random numbers") {
      val gen = AvroFaker(Schema.create(Schema.Type.FLOAT), new Random(0L))
      gen.generate().asInstanceOf[Float] shouldBe 0.730967787376657f +- 1e-7f
      gen.generate().asInstanceOf[Float] shouldBe 0.24053641567148587f +- 1e-7f
      gen.generate().asInstanceOf[Float] shouldBe 0.6374174253501083f +- 1e-7f
    }
  }

  describe("Generating Avro DOUBLE data") {
    it("should generate random numbers") {
      val gen = AvroFaker(Schema.create(Schema.Type.DOUBLE), new Random(0L))
      gen.generate().toString.toDouble shouldBe 0.730967787376657 +- 1e-14
      gen.generate().toString.toDouble shouldBe 0.24053641567148587 +- 1e-14
      gen.generate().toString.toDouble shouldBe 0.6374174253501083 +- 1e-14
    }
  }

  describe("Generating Avro BOOLEAN data") {
    it("should generate random booelans") {
      val gen = AvroFaker(Schema.create(Schema.Type.BOOLEAN), new Random(0L))
      gen.generate() shouldBe true
      gen.generate() shouldBe true
      gen.generate() shouldBe false
    }
  }

  describe("Generating Avro NULL data") {
    it("should only ever generate NULL") {
      val gen = AvroFaker(Schema.create(Schema.Type.NULL), new Random(0L))
      Option(gen.generate()) shouldBe None
      Option(gen.generate()) shouldBe None
      Option(gen.generate()) shouldBe None
    }
  }

}
