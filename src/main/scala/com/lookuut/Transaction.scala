package com.lookuut

import com.esri.core.geometry.Point
import org.joda.time.DateTime
import com.lookuut.utils.CSVParser


object Transaction {

	def stringToDouble(str : Option[String]) : Option[Double] = {
		try {

			if (!str.isEmpty) {
				Some(str.get.toDouble)
			} else {
				None
			}
			
		} catch {
			case e : NumberFormatException => None
		}
	}

	private val moscowCanoncialName = "MOSCOW"
	private val saintPetersburgCanoncialName = "ST-PETER"

	private val moscowVarians = Array(
									"MOSKVA", 
									"MOSCOW", 
									"MOSCOW REGION",
									"MOSKOW",
									"G MOSKVA"
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
									"SPB"
								)


	def getCity(city : Option[String]) : Option[String] = {
		val upperCaseCity = city.getOrElse("Unknown").toUpperCase
		val upperCaseCannoncialCity = {
			if (moscowVarians.filter(_ == upperCaseCity).size > 0) {
				Some(Transaction.moscowCanoncialName)
			} else if (piterVarians.filter(_ == upperCaseCity).size > 0) {
				Some(Transaction.saintPetersburgCanoncialName)
			} else {
				Some(upperCaseCity)
			}
		}
		upperCaseCannoncialCity
	}

	def getDate(sDate : Option[String]) : Option[DateTime] = {
		if (sDate.isEmpty) {
			None
		} else {
		    try {
		    	DateTime.parse(sDate.get)
		        Some(new DateTime(sDate.get))
		    } catch  {
		        case e : java.lang.UnsupportedOperationException => None
		        case e : java.lang.IllegalArgumentException => None
		    }
		}
	} 

	def parseString (line : String) : scala.collection.immutable.IndexedSeq[Option[String]] = {
		val row = CSVParser.parse(line,',', '"', '\\')
		
		val parsedRow = (0 until 18).map(i => {
			if (i < row.size) Some(row(i).trim) else None
		})

		parsedRow
	} 
	

	def parseTrainTransaction(line : String, index : Long) = {

		val parsedRow = parseString(line)
		val transcation = build(index,
			parsedRow(0),
			parsedRow(1),
			parsedRow(2),
			parsedRow(3),
			parsedRow(4),
			parsedRow(5),
			parsedRow(6),
			parsedRow(7),
			parsedRow(10),
			parsedRow(11),
			parsedRow(12),
			parsedRow(13),
			parsedRow(14),
			parsedRow(15)
		)

		val homePoint = new Point(
			stringToDouble(parsedRow(8)).getOrElse(0.0), 
			stringToDouble(parsedRow(9)).getOrElse(0.0)
		)

		val workPoint = new Point(
			stringToDouble(parsedRow(16)).getOrElse(0.0), 
			stringToDouble(parsedRow(17)).getOrElse(0.0)
		)
		
		TrainTransaction(
			transcation,
			homePoint,
			workPoint
		)
	}

	def parse(line : String, index : Long) : Transaction = {
		val parsedRow = parseString(line)
		build(index, 
			parsedRow(0),
			parsedRow(1),
			parsedRow(2),
			parsedRow(3),
			parsedRow(4),
			parsedRow(5),
			parsedRow(6),
			parsedRow(7),
			parsedRow(8),
			parsedRow(9),
			parsedRow(10),
			parsedRow(11),
			parsedRow(12),
			parsedRow(13)
		)
	}

	def build (id : Long,
		amount : Option[String],
		atm_address : Option[String],
		atmPointX : Option[String],
		atmPointY : Option[String],
		sCity : Option[String], 
		country : Option[String],
		currency : Option[String], 
		customer_id : Option[String], 
		mcc : Option[String],
		pos_address : Option[String],
		posPointX : Option[String],
		posPointY : Option[String],
		terminal_id : Option[String],
		sDate : Option[String]) : Transaction = {

		val city = Transaction.getCity(sCity)

		val terminalPoint = new Point(
								stringToDouble(atmPointX).getOrElse(0.0), 
								stringToDouble(atmPointY).getOrElse(0.0)
							)

		val posPoint = new Point(
								stringToDouble(posPointX).getOrElse(0.0), 
								stringToDouble(posPointY).getOrElse(0.0)
							)

		val transactionPoint = if (terminalPoint.getX > 0.0) terminalPoint else posPoint

		new Transaction(
			id,
			stringToDouble(amount),
			atm_address, 
			terminalPoint, 
			city, 
			country,//country
			currency,//currency
			customer_id.getOrElse(""),//customer_id
			mcc,
			pos_address,
			posPoint, 
			terminal_id,
			getDate(sDate),
			transactionPoint
		)
	}
}

@SerialVersionUID(123L)
class Transaction(
		val id : Long,
		val amount : Option[Double],
		val atm_address : Option[String],
		val atmPoint : Point,
		val city : Option[String], 
		val country : Option[String],
		val currency : Option[String], 
		val customer_id : String, 
		val mcc : Option[String],
		val pos_address : Option[String],
		val posPoint : Point,
		val terminal_id : Option[String],
		val transactionDate : Option[DateTime],
		val transactionPoint : Point 
	) extends Serializable {

	var nearTransactions = Set[Long]()

	def addNearTransactions (id : Long) {
		nearTransactions += id 
	}

	def getNearTransactions () : Set[Long] = {
		nearTransactions
	}

	override def toString = f"""Transaction([id=$id],[amount=$amount],[atm_address=$atm_address],[atmPoint=$atmPoint],[city=$city],[country=$country],[currency=$currency],[customer_id=$customer_id],[mcc=$mcc],[pos_address=$pos_address],[posPoint=$posPoint],[terminal_id=$terminal_id],[transactionDate=$transactionDate],[transactionPoint=$transactionPoint],[nearTransactions=$nearTransactions])"""
}
