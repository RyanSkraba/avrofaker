package com.skraba.avrofaker

import org.apache.avro.Schema

class NumberGaussStrategySpec extends WithTester {

  gaussStrategy(Tester.Int)
  gaussStrategy(Tester.Long)
  gaussStrategy(Tester.Float)
  gaussStrategy(Tester.Double)

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
}
