package com.skraba.avrofaker

import org.apache.avro.Schema;

/** Generates Avro data. */
class AvroFaker(spec: Schema) {
  def generate(): Any = spec
}
