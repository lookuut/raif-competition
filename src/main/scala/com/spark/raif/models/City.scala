/**
 *
 *
 * @author Lookuut Struchkov
 * @desc Transaction cities formatter
 * 	
 *
 */

package com.spark.raif.models

object City {

	private val MOSCOW = "MOSCOW"
	private val ST_PETER = "ST-PETER"

	private val moscowVarians = Array(
									"MOSKVA", 
									"MOSCOW", 
									"MOSCOW REGION",
									"MOSKOW",
									"G MOSKVA",
									"G. MOSKOW",
									"G. MOSKVA",
									"*MOSCOW",
									"77 - MOSCOW"
								)

	private val piterVarians = Array(
									"SANKT-PETERBU", 
									"ST-PETERSBURG", 
									"ST PETERSBURG", 
									"ST PETERBURG", 
									"ST-PETERBURG", 
									"SAINT PETERSB", 
									"ST.PETERSBURG", 
									"SANKT-PETERSB", 
									"SAINT-PETERSB", 
									"SANKT-PETERS",
									"ST.-PETERSBUR",
									"SANKT PETERBU",
									"SPB",
									"G SANKT-PETER",
									"G. SANKT-PETE"
								)

	def format(city : Option[String]) : Option[String] = {
		val upperCaseCity = city.getOrElse("Unknown").toUpperCase
		val upperCaseCannoncialCity = {
			if (moscowVarians.filter(_ == upperCaseCity).size > 0) {
				Some(City.MOSCOW)
			} else if (piterVarians.filter(_ == upperCaseCity).size > 0) {
				Some(City.ST_PETER)
			} else {
				Some(upperCaseCity)
			}
		}
		upperCaseCannoncialCity
	}
}