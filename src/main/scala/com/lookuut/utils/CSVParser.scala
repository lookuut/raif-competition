package com.lookuut.utils

import scala.collection.mutable.ArrayBuffer


object CSVParser {

	def parse(strIn : String, delimeter: Char, quote : Char, escape: Char) : ArrayBuffer[String] = {
		
		if (strIn.length == 0) {
			return ArrayBuffer[String]()
		}

		var str = strIn
		
		if (strIn.contains("\"\"\"\"")) {
			str = strIn.replace("\"\"\"\"", "\\\"")
		} 

		if (str.contains("\"\"\"Caf&#233\",\"-bar Campus\"\"\"")) {
			str = str.replace("\"\"\"Caf&#233\",\"-bar Campus\"\"\"" , "\"Caf&#233\", \"-bar Campus\"")
		}

		if (str.contains("\"\"\"OSN.PR\",\" ZZ\"\"\"")) {
			str = str.replace("\"\"\"OSN.PR\",\" ZZ\"\"\"", "\"OSN.PR\",\"ZZ\"")
		}

		if (str.contains("""TR BLD KAFE ""TU\BOLSHAYA TARL\423634    RUSRUS""")) {
			str = str.replace("""TR BLD KAFE ""TU\BOLSHAYA TARL\423634    RUSRUS""", """TR BLD KAFE TU\BOLSHAYA TARL\423634    RUSRUS""")
		}

		if (str.contains("""ALESYA"",MAG.""PELIKAN"",70 LET OKTYABRYA 41A\TOGLIATTI\445000    RUSRUS""")) {
			str = str.replace("""ALESYA"",MAG.""PELIKAN"",70 LET OKTYABRYA 41A\TOGLIATTI\445000    RUSRUS""", """ALESYA,MAG.PELIKAN,70 LET OKTYABRYA 41A\TOGLIATTI\445000    RUSRUS""")
		}
		
		if (str.contains("""207 SHOP "" LIGA""\KARAGANDA\KAZ       KAZKAZ""")) {
			str = str.replace("""207 SHOP "" LIGA""\KARAGANDA\KAZ       KAZKAZ""", """207 SHOP  LIGA\KARAGANDA\KAZ       KAZKAZ""")
		}

		if (str.contains("""TTS ""KROKUS SI\MOSCOW\123592    RUSRUS""")) {
			str = str.replace("""TTS ""KROKUS SI\MOSCOW\123592    RUSRUS""", """TTS KROKUS SI\MOSCOW\123592    RUSRUS""")
		}

		if (str.contains("""aya""""")) {
			str = str.replace("""aya""""", """aya""")
		}

		if (str.contains("""""MOSKVA-MINSK""""")) {
			str = str.replace("""""MOSKVA-MINSK""""", """MOSKVA-MINSK""")
		}

		if (str.contains("""""RADON""""")) {
			str = str.replace("""""RADON""""", """RADON""")
		}

		if (str.contains("""""Baltija""""")) {
			str = str.replace("""""Baltija""""", """Baltija""")
		}

		if (str.contains("""""MOSKVA-SPB""""")) {
			str = str.replace("""""MOSKVA-SPB""""", """MOSKVA-SPB""")
		}

		if (str.contains("""NSKAYA""""")) {
			str = str.replace("""NSKAYA""""", """NSKAYA""")
		}

		if (str.contains("RUS\"\"\"")) {
			str = str.replace("RUS\"\"\"", "RUS\"")
		}

		
		var result = ArrayBuffer[String]()
		var buffer = ArrayBuffer[Char]()
		var isQuoted : Boolean = false
		var i : Int = 0
		var isEscape = false
		var quoteCount = 0
		var inQuote = false
		for (sign <- str) {
			if (!isQuoted) {
				if (str(i) == quote && i > 0 && str(i - 1) != escape) {
					isQuoted = true
					isEscape = true
					quoteCount += 1
				}
			} else if (isQuoted) {
				
				if (!inQuote && isQuoted && str(i) == quote && i > 0 && str(i - 1) == quote) {
					quoteCount += 1
				}

				if (!inQuote && isQuoted && str(i) != quote && i > 0 && str(i - 1) == quote) {
					inQuote = true
				}

				if (inQuote && isQuoted && str(i) == quote && i > 0 && str(i - 1) != escape) {
					quoteCount -= 1
				}

				if (str(i) == quote && i > 0 && str(i - 1) != escape && 
					(
						i + 1 == str.length || //end of line 
						str(i + 1) == delimeter //or delimeter at the end @TODO do with space params
					)
					&& 
					quoteCount == 0
				) {
					isQuoted = false
					isEscape = true
					inQuote = false
				}
			} 


			if (str(i) == delimeter && !isQuoted) {
				result += (buffer.mkString(""))
				buffer = ArrayBuffer[Char]()
			} else if(!isEscape) {
				buffer += str(i)
			}

			i += 1
			isEscape = false
		}
		result += buffer.mkString("")
	}
}