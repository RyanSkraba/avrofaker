package com.skraba.avrofaker

class NumberValueStrategySpec extends WithTester {

  valueStrategy(Tester.Int)
  valueStrategy(Tester.Long)
  valueStrategy(Tester.Float)
  valueStrategy(Tester.Double)

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

}
