package com.skraba.avrofaker

import org.apache.avro.Schema

class NumberRandomStrategySpec extends WithTester {

  randomStrategy(Tester.Int)
  randomStrategy(Tester.Long)
  randomStrategy(Tester.Float)
  randomStrategy(Tester.Double)

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
}
