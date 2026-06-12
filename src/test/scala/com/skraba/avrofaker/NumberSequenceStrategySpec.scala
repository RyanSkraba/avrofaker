package com.skraba.avrofaker

import org.apache.avro.Schema

class NumberSequenceStrategySpec extends WithTester {
  sequenceStrategy(Tester.Int)
  sequenceStrategy(Tester.Long)
  sequenceStrategy(Tester.Float)
  sequenceStrategy(Tester.Double)

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
}
