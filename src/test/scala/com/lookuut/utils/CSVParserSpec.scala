package com.lookuut.uitls

import com.lookuut.utils.CSVParser

object CSVParserSpec extends org.specs2.mutable.Specification {

  "CSVParser compute()" should {

    "Test parser" in {
      CSVParser.parse("4234234,423423", ',', '"', '\\').apply(0) mustEqual "4234234"
      CSVParser.parse("""4234234,423423,выалодыуаоыуа,"вфцвб,dawdwd,dawdawd",344""", ',', '"', '\\').apply(0) mustEqual "4234234"
    }
  }
}



