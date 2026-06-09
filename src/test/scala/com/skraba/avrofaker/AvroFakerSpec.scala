package com.skraba.avrofaker

import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import scala.util.Random

class AvroFakerSpec extends WithTester {

  def randomStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the random strategy") {

      /** Expected results over the default random range */
      val defaults = it.sType match {
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
        """"<TYPE>"""" -> defaults,
        """{"type": "<TYPE>"}""" -> defaults
      )

      it("should generate a random number when the random strategy is explicitly set")(
        """{"faker": "random"}""" -> defaults,
        """{"faker": {"faker": "random"}}""" -> defaults
      )

      it("should generate a random number when the random strategy is unset")(
        """{"faker": {}}""" -> defaults
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

        it("should generate between -1 and 2")(
          """{"min": -1, "max": 2}""" -> Seq(-1, 0, 0, 1, 1, 1, 1, -1, -1, 1)
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

        it("should generate between -1 and 2")(
          """{"min": -1, "max": 2}""" -> Seq(1.1929033621299712, -0.2783907529855424, 0.9122522760503249,
            0.6513110153529018, 0.7926358333916053, -3.44801570050679e-4, 0.1555675542221555, 1.954524620599427,
            1.6375475536174404, 1.823747538446343)
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
      it("should start at the specified value")(
        """{"min": 10,  "step": 1}""" -> Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
      )
      it("should rotate before reaching the end")(
        """{"min": 1, "max": 4, "step": 1}""" -> Seq(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
      )
      it("should have a configurable step before reaching the end")(
        """{"min": -5, "max": 5, "step": 3}""" -> Seq(-5, -2, 1, 4, -3, 0, 3, -4, -1, 2, -5, -2, 1, 4, -3)
      )

      if (!it.isIntegral) {
        it("should start at the specified value at fractional values") {
          """{"max": 0.75, "step": -0.25}""" -> Seq(0.5, 0.25, 0.0, -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75)
        }

        it("should rotate before reaching the end with fractional values") {
          """{"min": 0.75, "max": 2.5, "step": 0.5}""" -> Seq(0.75, 1.25, 1.75, 2.25, 1.0, 1.5, 2.0, 0.75, 1.25, 1.75)
        }
      }

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
        it("should go to infinity at the default max value", roundTrip = 6)(
          // We have to disable the roundtrip for numbers that are NaN because NaN never equals NaN
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

  def weightsStrategy[T](it: NumericTester[T]): Unit = {
    describe(s"Generate ${it.sType} with the weights strategies") {

      val expected = Seq(1569741360, 1785505948, 516548029, 1302116447, 1368843515)

      it("should generate positive numbers when weights aren't specified and max isn't used")(
        """{"faker": "weights"}""" -> expected,
        """{"faker": "weights", "weights": []}""" -> expected,
        """{"faker": "weights", "weights": 1}""" -> expected,
        """{"faker": "weights", "weights": [1,1,1,1]}""" -> expected,
        """{"weights": []}""" -> expected,
        """{"weights": 1}""" -> expected,
        """{"weights": [1,1,1,1]}""" -> expected
      )

      it("should only choose prime numbers below 10")(
        """{"max": 10, "weights": [0, 0, 1, 1, 0, 1, 0, 1, 0, 0]}""" -> new it.Dist(
          2 -> 972,
          3 -> 1002,
          5 -> 1011,
          7 -> 1015
        )
      )

      it("should weight the single digit numbers")(
        """{"max": 10, "weights": [1, 1, 1, 9]}""" -> new it.Dist(58, 73, 49, 465, 47, 66, 46, 70, 57, 69),
        """{"max": 10, "weights": [7, 3.5, 3.5]}""" -> new it.Dist(301, 145, 165, 57, 54, 35, 47, 49, 52, 39)
      )

      if (it.isIntegral) {
        it("should weight picking 999 over single digits")(
          // TODO: Why is 999 not getting picked ?
          """{"oneof": [999, 1], "index": {"weights": [1000, 1]}}""" -> new it.Dist(1 -> 1000),
          // TODO: Why is 999 only getting 50%?
          """{"oneof": [999, {"min": 0, "max": 4}], "index": {"weights": [1000, 1]}}""" -> new it.Dist(
            0 -> 139,
            1 -> 127,
            2 -> 116,
            3 -> 130,
            999 -> 488
          )
        )
      }

      val expected5Max_1_5 = new it.Dist(1002, 4942, 1018, 1056, 982)

      it("should generate small numbers with the expected frequency")(
        """{"weights": [5,1], "max": 2}""" -> new it.Dist(4993, 1007),
        """{"weights": [5], "max": 2}""" -> new it.Dist(4993, 1007),
        """{"weights": [2.5,0.5], "max": 2}""" -> new it.Dist(4993, 1007),
        """{"weights": [5,1,1,1,1], "max": 2}""" -> new it.Dist(4993, 1007),
        """{"weights": [5,1,1000,1000,1000], "max": 2}""" -> new it.Dist(4993, 1007),
        """{"weights": [1,5], "max": 2}""" -> new it.Dist(1038, 4962),
        """{"weights": [1,5], "max": 3}""" -> new it.Dist(1038, 5002, 960),
        """{"weights": [1,5], "max": 5}""" -> expected5Max_1_5,
        """{"weights": [1,5,1], "max": 5}""" -> expected5Max_1_5,
        """{"weights": [1,5,1,1], "max": 5}""" -> expected5Max_1_5,
        """{"weights": [1,5,1,1,1], "max": 5}""" -> expected5Max_1_5,
        """{"weights": [1,5,1,1,1,1,1,1], "max": 5}""" -> expected5Max_1_5
      )

      it("should get the correct distribution among 5 numbers with 5 weights")(
        """{"weights": [5,4,3,2,1], "max": 5}""" -> new it.Dist(4994, 3950, 3053, 1989, 1014)
      )

      it("should get the correct distribution among 5 numbers with 4 weights")(
        """{"weights": [5,4,3,2], "max": 5}""" -> new it.Dist(4994, 3950, 3053, 1989, 1014)
      )

      it("should get the correct distribution among 8 numbers with 4 weights")(
        """{"weights": [5,4,3,2], "max": 8}""" ->
          new it.Dist(4636, 3879, 2819, 1885, 921, 932, 912, 1016)
      )
    }
  }
  weightsStrategy(Tester.Int)
  weightsStrategy(Tester.Long)
  weightsStrategy(Tester.Float)
  weightsStrategy(Tester.Double)

  describe("Generating Avro RECORD data") {
    Tester.Record("should create an simple record")(
      """{  "type": "record",
       |  "name": "A",
       |  "fields": [
       |    {"name": "a1", "type": "int", "step": 1},
       |    {"name": "a2", "type": "string", "expression": "#{Name.first_name}"}
       |  ]
       |}""".stripMargin -> Seq(
        """{"a1": 0, "a2": "Kit"}""",
        """{"a1": 1, "a2": "Barry"}""",
        """{"a1": 2, "a2": "Dionne"}""",
        """{"a1": 3, "a2": "Carola"}""",
        """{"a1": 4, "a2": "Jessenia"}""",
        """{"a1": 5, "a2": "Roger"}""",
        """{"a1": 6, "a2": "Joleen"}""",
        """{"a1": 7, "a2": "Oliver"}""",
        """{"a1": 8, "a2": "Robbie"}""",
        """{"a1": 9, "a2": "Jean"}"""
      )
    )
  }

  describe("Generating Avro ENUM data") {
    Tester.Enum("should pick symbols randomly by default")(
      """{"name": "abc", "symbols": ["A", "B", "C"]}""",
      new Tester.Enum.Dist("A" -> 1023, "B" -> 982, "C" -> 995)
    )

    Tester.Enum("should pick a gaussian distribution of elements")(
      """{"name": "abc", "symbols": ["A", "B", "C", "D", "E", "F", "G"], "index": {"mean": 3.5, "stddev": 1}}""",
      new Tester.Enum.Dist("A" -> 61, "B" -> 626, "C" -> 2409, "D" -> 3857, "E" -> 2406, "F" -> 595, "G" -> 46)
    )

    Tester.Enum("should step through the symbols")(
      """{"name": "abc", "symbols": ["A", "B", "C"], "index": {"step": 1}}""" -> Seq("A", "B", "C", "A", "B", "C"),
      """{"name": "abc", "symbols": ["A", "B", "C"], "step": 1, "index": {}}""" -> Seq("A", "B", "C", "A", "B", "C")
    )
  }

  describe("Generating Avro ARRAY data") {
    val defaultInts = Seq(
      Seq(-845367200, -965429156, 604591031),
      Seq(1316484263, -1333195387, 377907320, 1350546348, 1514351261),
      Seq(19810223, -1923091611, 613975862),
      Seq(1023700812, -1101159093, -1114661780),
      Seq(-2119981586, -1016405231, -1106653855, 178375143),
      Seq(-596233542, -2009457131, 2067936182, 1432048073),
      Seq(-395271067, 771712312, -1622511698, -1905717566, 1742422067),
      Seq(177782380, 1486687917, 962469399)
    )

    Tester.Array("should create arrays of its element type with a default size")(
      """{"items": "int"}""" -> defaultInts,
      """{"items": "int", "length": {"min": 3, "max": 6}}""" -> defaultInts
    )

    Tester.Array("should create arrays of its element type with a constant size")(
      """{"items": "int", "length": 2}""" -> Seq(
        Seq(-1630935619, -1483802595),
        Seq(-864264928, -530909147),
        Seq(1041189272, -2097915773),
        Seq(-483282681, 1863963771)
      )
    )

    Tester.Array("should create arrays of its element type with a constant size")(
      """{"items": {"type": "int", "step": 1}, "length": 2}""" -> Seq(Seq(0, 1), Seq(2, 3), Seq(4, 5), Seq(6, 7)),
      """{"items": {"type": "int", "index": {"step": 1}, "oneof": [{"min": 100, "max": 110}, {"step": 1}, 999]}, "length": 3}""" -> Seq(
        Seq(100, 0, 999),
        Seq(108, 1, 999),
        Seq(109, 2, 999),
        Seq(107, 3, 999)
      )
    )

    Tester.Array("should create STRING pairs of a constant size")(
      """{"items": "string", "length" : 2}""" -> Seq(Seq("CC", "zL"), Seq("NH", "BF"), Seq("Hu", "Rv"), Seq("bI", "1i"))
    )

    Tester.Array("should create STRING triples containing a single character")(
      """{"items": {"type": "string", "length": 1}, "length" : 3}""" -> Seq(Seq("C", "C", "z"), Seq("L", "N", "H"))
    )

    Tester.Array("should create bounded double arrays from 0 to 10 elements")(
      """{ "min": 0, "max": 10, "items": "double"}""" -> Seq(
        Seq(),
        Seq(2.4053641567148585, 6.374174253501082, 5.504370051176339, 5.975452777972018, 3.3321839947664977,
          3.851891847407185, 9.84841540199809, 8.791825178724801),
        Seq(1.7597680203548016),
        Seq(1.2889715087377673, 1.4660165764651822, 0.23238122483889456, 5.467397571984655, 9.644868606768501,
          1.0449068625097169, 6.251463634655593, 4.107961954910618)
      )
    )
  }

  describe("Generating Avro MAP data") {
    val defaultInts = Seq(
      Map("CzLNHBFHuR" -> 1316484263, "1iI19WjGGR" -> 49903503, "WutFRZvWeb" -> 1023700812),
      Map(
        "vBbFbQPNB7" -> -1052968156,
        "SWpBejTwv1" -> 575571244,
        "Pyuxbr589w" -> -766178373,
        "Hfyqts0coJ" -> 484847823,
        "S2SuiHrAOB" -> -1218980251
      ),
      Map("7sjiP63hvG" -> 1382431975, "fc8BAUaequ" -> 1831433050, "pB2TWOPFt1" -> -1067015046),
      Map("zxd8prf0oY" -> -348950749, "jFIXxlrCzE" -> 752288255, "X5hLVIpJ1E" -> -1567159908),
      Map(
        "m0zER7Z50Y" -> 430605778,
        "DuJsQsMkp0" -> -1620592409,
        "uvgbCx2TEr" -> -871983775,
        "ut0Uk2wzXd" -> 758174477
      ),
      Map("cKN0hZKzaP" -> 86391971, "oP2Wz64nwk" -> 1009233430, "TyeMd0v8N5" -> 458034654),
      Map(
        "V6kIx0Ngqk" -> -1333050422,
        "m3uF6DWDU0" -> -654861888,
        "4vR6Yu7uds" -> -973694788,
        "BNrJEUwjBg" -> -697131850
      ),
      Map("09HViqK07H" -> -560660383, "aWih0Og1ZY" -> 362481239, "pNwuZuiGR2" -> -2054498974)
    )

    Tester.Map("should create maps with the default key and size")(
      """{"values": "int"}""" -> defaultInts,
      """{"values": "int", "key": {"length": 10}, "length": {"min": 3, "max": 6}}""" -> defaultInts
    )

    Tester.Map("should create maps with the default key and constant size")(
      """{"values": "int", "length": 2}""" -> Seq(
        Map("CC" -> 295101692, "HB" -> 604591031),
        Map("Rv" -> 1041189272, "iI" -> 377907320),
        Map("jG" -> 1388566476, "UN" -> 19810223),
        Map("FR" -> 1342491603, "eb" -> 1023700812)
      ),
      """{"values": {"type": "int", "index": {"step": 1}, "oneof": [{"min": 100, "max": 110}, {"step": 1}, 999]}, "length": 3}""" -> Seq(
        Map("CCz" -> 107, "NHB" -> 0, "FHu" -> 999),
        Map("Rvb" -> 102, "1iI" -> 1, "19W" -> 999),
        Map("jGG" -> 107, "8UN" -> 2, "Wut" -> 999),
        Map("FRZ" -> 107, "Web" -> 3, "pA5" -> 999)
      )
    )

    Tester.Map("should create maps setting the map size and the key size to a constant 2, and a value sequence")(
      """{"values": {"type": "int", "step": 1}, "length": 2}""" -> Seq(
        Map("CC" -> 0, "zL" -> 1),
        Map("NH" -> 2, "BF" -> 3),
        Map("Hu" -> 4, "Rv" -> 5),
        Map("bI" -> 6, "1i" -> 7)
      )
    )

    Tester.Map("should create maps setting the map size and the key size to a constant 2 with random STRING values")(
      """{"values": "string", "length" : 2}""" -> Seq(
        Map("CC" -> "zL", "NH" -> "BF"),
        Map("Hu" -> "Rv", "bI" -> "1i"),
        Map("I1" -> "9W", "jG" -> "GR"),
        Map("8U" -> "NW", "ut" -> "FR")
      )
    )

    Tester.Map("should create maps with 3 STRING keys of with size 1 STRING values")(
      """{"values": {"type": "string", "length": 1}, "length" : 3}""" -> Seq(
        Map("CCz" -> "L", "NHB" -> "F", "HuR" -> "v"),
        Map("bI1" -> "i", "I19" -> "W", "jGG" -> "R"),
        Map("8UN" -> "W", "utF" -> "R", "ZvW" -> "e"),
        Map("bpA" -> "5", "WHf" -> "y", "qts" -> "0")
      )
    )

    Tester.Map("should create maps with 0 to 10 keys containing doubles from 0 to 10")(
      """{"min": 0, "max": 10, "values": "double"}""" -> Seq(
        Map(),
        Map(
          "" -> 7.462414053223306,
          "R8UN" -> 0.013866804054343262,
          "iI19W" -> 2.7495396603548485,
          "LNHBFHuRv" -> 3.851891847407185,
          "bp" -> 7.763122912749325,
          "0coJ" -> 4.122177440926348,
          "FRZ" -> 2.52883130686676
        ),
        Map(
          "" -> 3.387696535357536,
          "yuxbr58" -> 7.223571191888487,
          "uvBbF" -> 6.125811047098681,
          "NB7ZuKSWp" -> 7.7131296617067955,
          "JzS2SuiH" -> 6.895039878550204,
          "Twv1N" -> 9.453332389596289
        ),
        Map("sjiP63h" -> 3.162993026242317)
      )
    )

    Tester.Map("should create maps with DataFaker expression keys")(
      """{"values": {"type": "int", "min": 10, "max": 100}, "key": {"expression": "#{Address.country}"}}""" -> Seq(
        Map("Luxembourg" -> 57, "France" -> 45, "Moldova" -> 93),
        Map("Andorra" -> 84, "Trinidad and Tobago" -> 79, "Mongolia" -> 87, "Zambia" -> 31, "Swaziland" -> 27),
        Map(
          "Liechtenstein" -> 24,
          "Saint Pierre and Miquelon" -> 45,
          "Sierra Leone" -> 25,
          "Bahrain" -> 14,
          "Portugal" -> 92
        ),
        Map("Gabon" -> 30, "India" -> 28, "Kenya" -> 87, "Gambia" -> 84, "Central African Republic" -> 23)
      )
    )
  }

  describe("Generating Avro UNION data") {
    val expected = Seq(null, null, -1483802595, null, -1431902571, 1041189272, null, null, -483282681, 1388566476)

    Tester.Union("should generate data from the union types with equal probability")(
      """["int", "null"]""" -> expected,
      """[{"type": "int"}, "null"]""" -> expected,
      """["int", {"type": "null"}]""" -> expected,
      """[{"type": "int"}, {"type": "null"}]""" -> expected,
      """[{"type": "int", "faker": "random"}, {"type": "null"}]""" -> expected
    )

    Tester.Union("should allow the index to be specified in the first union element")(
      """[{
        |  "type": "string",
        |  "union": {"index": {"step": 1}}
        |}, {
        |  "type": "int",
        |  "step": 2
        |}]""".stripMargin -> Seq("CCzLNHBFHu", 0, "RvbI1iI19W", 2, "jGGR8UNWut", 4, "FRZvWebpA5", 6, "WHfyqts0co", 8)
    )
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
      // TODO: """{"oneof": ["ABC", "BCD", "CDE"], "step": 1}""" -> Seq("ABC", "BCD", "CDE", "ABC", "BCD", "CDE")
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

  describe("Generate BYTES with the random strategy") {
    val expected = Seq(
      "OFHZ1HrLkz2+cDmb9sktozo=",
      "AyX0HT66+JhtpxLIK81NVUvw",
      "TenvnC+THvxYD5r7CBsS4Qex6AXytPXw8dAM",
      "cJIcUFhn/yD2qDNemK+HJThVhrQf7/IFtOBaAAg="
    )

    Tester.Bytes("should generate with the defaults")(
      """"bytes"""" -> expected,
      """{"type": "bytes"}""" -> expected,
      """{"length": {"min": 16, "max": 33}}""" -> expected
    )

    Tester.Bytes("should generate a constant length array")(
      """{"length": 8}""" -> Seq("YLQguzhR2dQ=", "esuTPb5wOZs=", "9sktozrwHU8=", "t3DpjAMl9B0=")
    )

    Tester.Bytes("should generate an increasing array")(
      """{"length": {"step": 1, "max": 32}}""" -> Seq(
        "",
        "YA==",
        "OFE=",
        "esuT",
        "vnA5mw==",
        "9sktozo=",
        "t3DpjAMl",
        "Prr4mG2nEg==",
        "K81NVUvwtUA=",
        "I8KbYk3p75wv",
        "WA+a+wgbEuEHsQ=="
      )
    )
  }

  describe("Generate FIXED with the random strategy") {
    val expected = Seq("YLQguw==", "OFHZ1A==", "esuTPQ==", "vnA5mw==")

    Tester.Fixed("should generate with the defaults")(
      """{"type": "fixed", "name": "f4", "size": 4}""" -> expected,
      """{"name": "f4", "size": 4}""" -> expected
    )
  }

  describe("Generate BOOLEAN data") {
    val expected = Seq(true, true, false, true, true, false, true, false, true, true)

    Tester.Boolean("should generate with the defaults")(
      """"boolean"""" -> expected,
      """{"type": "boolean"}""" -> expected
    )

    Tester.Boolean("should alternate between false and true")(
      """{"step": 1}""" -> Seq(false, true, false, true, false, true, false, true, false, true),
      """{"step": 1, "start": true}""" -> Seq(true, false, true, false, true, false, true, false, true, false),
      """{"step": 1, "start": "true"}""" -> Seq(true, false, true, false, true, false, true, false, true, false)
    )
  }

  describe("Generate NULL data") {
    Tester.Null("should generate with the defaults")(
      """"null"""" -> Seq.fill(10)(null),
      """{"type": "null"}""" -> Seq.fill(10)(null)
    )
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
