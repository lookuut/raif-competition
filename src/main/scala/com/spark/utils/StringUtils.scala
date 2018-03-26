package com.spark.utils

object StringUtils {

	implicit class RichString(val s: String) {
		
		def isFloatNumber : Boolean = s.matches("[+-]?\\d+.?\\d+") 
		def isInteger : Boolean = s.matches("[+-]?\\d+")

		def toDoubleSafe(failedValue : Double = 0.0) : Double = if (s.isFloatNumber) s.toDouble else failedValue
	}
}