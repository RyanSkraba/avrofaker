package com.skraba.avrofaker

class NumberAggregateStrategySpec extends WithTester {

  aggregateStrategies(Tester.Int)
  aggregateStrategies(Tester.Long)
  aggregateStrategies(Tester.Float)
  aggregateStrategies(Tester.Double)

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
}
