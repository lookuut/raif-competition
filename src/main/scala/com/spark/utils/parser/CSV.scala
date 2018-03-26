package com.spark.utils.parser

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object CSV {

	def parse(data : RDD[String]) : RDD[Array[String]] = {
		data.map(t => CSV.parseString(t, ',', '"', '\\'))
	}

	def parseString(str : String, delimeter: Char, quote : Char, escape: Char) : Array[String] = {
		
		if (str.length == 0) {
			return Array()
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
		result.toArray
	}
}