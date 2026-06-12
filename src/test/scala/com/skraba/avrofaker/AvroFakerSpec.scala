package com.skraba.avrofaker

import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{Schema, SchemaBuilder}
import scala.util.Random

class AvroFakerSpec extends WithTester {

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
