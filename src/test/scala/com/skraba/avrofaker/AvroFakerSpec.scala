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

  describe("Generating Avro STRING data") {
    it("should create ten character strings by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.STRING), new Random(0L))
      gen.generate() shouldBe "CCzLNHBFHu"
      gen.generate() shouldBe "RvbI1iI19W"
      gen.generate() shouldBe "jGGR8UNWut"
    }
  }

  describe("Generating Avro INT data") {
    it("should create a sequence by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.INT), new Random(0L))
      gen.generate() shouldBe 0
      gen.generate() shouldBe 1
      gen.generate() shouldBe 2
    }
  }

  describe("Generating Avro LONG data") {
    it("should create a sequence by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.LONG), new Random(0L))
      gen.generate() shouldBe 0L
      gen.generate() shouldBe 1L
      gen.generate() shouldBe 2L
    }
  }

  describe("Generating Avro DOUBLE data") {
    it("should create a sequence by default") {
      val gen = AvroFaker(Schema.create(Schema.Type.DOUBLE), new Random(0L))
      gen.generate().toString.toDouble shouldBe 0.730967787376657 +- 1e-14
      gen.generate().toString.toDouble shouldBe 0.24053641567148587 +- 1e-14
      gen.generate().toString.toDouble shouldBe 0.6374174253501083 +- 1e-14
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
