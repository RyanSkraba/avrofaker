package com.skraba.avrofaker

class NumberWeightsStrategySpec extends WithTester {

  weightsStrategy(Tester.Int)
  weightsStrategy(Tester.Long)
  weightsStrategy(Tester.Float)
  weightsStrategy(Tester.Double)

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
          """{"oneof": [999, 1], "index": {"weights": [0,1]}}""" -> new it.Dist(1 -> 1000),
          """{"oneof": [999, 1], "index": {"weights": [1,1]}}""" -> new it.Dist(1 -> 492, 999 -> 508),
          """{"oneof": [999, 1], "index": {"weights": [2,1]}}""" -> new it.Dist(1 -> 337, 999 -> 663),
          """{"oneof": [999, 1], "index": {"weights": [10, 1]}}""" -> new it.Dist(1 -> 90, 999 -> 910),
          """{"oneof": [999, 1], "index": {"weights": [100, 1]}}""" -> new it.Dist(1 -> 8, 999 -> 992),
          """{"oneof": [999, {"min": 0, "max": 4}], "index": {"weights": [10, 1]}}""" -> new it.Dist(
            0 -> 15,
            1 -> 31,
            2 -> 18,
            3 -> 13,
            999 -> 923
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
}
