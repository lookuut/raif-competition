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

		var result = ArrayBuffer[String]()
		var buffer = ArrayBuffer[Char]()
		var isQuoted : Boolean = false
		var i : Int = 0
		var isEscape = false
		for (sign <- str) {
			
			if (!isQuoted) {
				if (str(i) == quote && i > 0 && str(i - 1) != escape) {
					isQuoted = true
					isEscape = true
				}
			} else if (isQuoted) {
				if (str(i) == quote && i > 0 && str(i - 1) != escape && 
					(
						i + 1 == str.length || //end of line 
						str(i + 1) == delimeter //or delimeter at the end @TODO do with space params
					)
				) {
					isQuoted = false
					isEscape = true
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