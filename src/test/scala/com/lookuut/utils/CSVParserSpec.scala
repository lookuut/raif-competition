package com.lookuut.uitls

import com.lookuut.utils.RichPoint
import com.lookuut.utils.CSVParser
import com.esri.core.geometry.Point

object CSVParserSpec extends org.specs2.mutable.Specification {

  "CSVParser compute()" should {

    "Test parser" in {
    	CSVParser.parse("""4234234,423423,выалодыуаоыуа,"вфцвб,dawdwd,dawdawd",344""", ',', '"', '\\').apply(3) mustEqual "вфцвб,dawdwd,dawdawd"
    }
  }
}



