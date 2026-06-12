package com.skraba.avrofaker

class NumberOneOfStrategySpec extends WithTester {

  oneofStrategy(Tester.Int)
  oneofStrategy(Tester.Long)
  oneofStrategy(Tester.Float)
  oneofStrategy(Tester.Double)

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

}
