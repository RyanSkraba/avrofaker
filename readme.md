AvroFaker
==============================================================================

[![Java CI with Maven](https://github.com/RyanSkraba/avrofaker/actions/workflows/maven.yml/badge.svg)](https://github.com/RyanSkraba/avrofaker/actions/workflows/maven.yml)

AvroFaker creates generic Avro data from an annotated Avro schema.

As a basic example, to generate random integers between 100 and 200:

```scala
val schema = """{"type": "int", "min": 100, "max": 200}"""
val faker = AvroFaker(new org.apache.avro.Schema.Parser().parse(schema))

// prints 194 134 191 104 181 117 148 133 122 174 130
for (_ <- 0 to 10) print(s"${faker.generate()} ")
```

Every schema and subschema can be annotated, so you can create awesome, customizable fake data for testing!

Using
------------------------------------------------------------------------------

You can import the library into your project from [maven central](https://central.sonatype.com/artifact/com.tinfoiled/docopt4s_2.13):

```xml
<dependency>
  <groupId>com.skraba</groupId>
  <artifactId>avrofaker</artifactId>
  <version>0.0.0</version>
</dependency>
```

Building
------------------------------------------------------------------------------

```sh
# Build, format and run all tests
mvn spotless:apply clean verify
```
