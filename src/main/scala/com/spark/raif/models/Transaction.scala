/**
 *
 *
 * @author Lookuut Struchkov
 * @desc Transaction
 * 	
 *
 */

package com.spark.raif.models

import org.apache.spark.rdd.RDD

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.esri.core.geometry.Point

import com.spark.raif.Consts
import com.spark.utils.parser.CSV
import com.spark.utils.StringUtils._


object Transaction {
	private val paramsMaxCount = 18
	private val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

	def getDate(sDate : Option[String]) : Option[DateTime] = {
		try {
			DateTime.parse(sDate.get)
			Some(new DateTime(sDate.get))
		} catch {
			case e : java.lang.UnsupportedOperationException => None
			case e : java.lang.IllegalArgumentException => None
		}
	} 

	/**
	 * 	@TODO use config file for special address cases
	 **/
	
	private val specialAddressCases = Map(
		"\"\"\"\"" -> "\\\"",
		"\"\"\"Caf&#233\",\"-bar Campus\"\"\"" -> "\"Caf&#233\", \"-bar Campus\"",
		"\"\"\"OSN.PR\",\" ZZ\"\"\"" -> "\"OSN.PR\",\"ZZ\"",
		"""TR BLD KAFE ""TU\BOLSHAYA TARL\423634    RUSRUS""" -> """TR BLD KAFE TU\BOLSHAYA TARL\423634    RUSRUS""",
		"""ALESYA"",MAG.""PELIKAN"",70 LET OKTYABRYA 41A\TOGLIATTI\445000    RUSRUS""" -> """ALESYA,MAG.PELIKAN,70 LET OKTYABRYA 41A\TOGLIATTI\445000    RUSRUS""",
		"""207 SHOP "" LIGA""\KARAGANDA\KAZ       KAZKAZ""" -> """207 SHOP  LIGA\KARAGANDA\KAZ       KAZKAZ""",
		"""TTS ""KROKUS SI\MOSCOW\123592    RUSRUS""" -> """TTS KROKUS SI\MOSCOW\123592    RUSRUS""",
		"""aya""""" -> """aya""",
		"""""MOSKVA-MINSK""""" -> """MOSKVA-MINSK""",
		"""""RADON""""" -> """RADON""",
		"""""Baltija""""" -> """Baltija""",
		"""""MOSKVA-SPB""""" -> """MOSKVA-SPB""",
		"""NSKAYA""""" -> """NSKAYA""",
		"RUS\"\"\"" -> "RUS\""
	)

	def parseString (dirtyLine : String, paramsCount : Int) : Seq[Option[String]] = {

		val clearLine = (dirtyLine +: specialAddressCases.keys.toList).
						reduceLeft(
							(line, pattern) => 
								if(line.contains(pattern)) 
									line.replace(pattern, specialAddressCases.get(pattern).get) 
								else line
						)
		val row = CSV.parseString(clearLine, ',', '"', '\\')
		
		val parsedRow = (0 until paramsCount).map(i => {
			if (i < row.size) Some(row(i).trim) else None
		})

		parsedRow
	} 
	
	def parse(line : String, id : Long) : Transaction = {
		val parsedRow = parseString(line, paramsMaxCount)

		init(id, 
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

	def init (id : Long,
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
		sDate : Option[String]
	) : Transaction = {

		val city = City.format(sCity)

		val terminalPoint = new Point(
								atmPointX.getOrElse("0.0").toDoubleSafe(),
								atmPointY.getOrElse("0.0").toDoubleSafe()
							)

		val posPoint = new Point(
								posPointX.getOrElse("0.0").toDoubleSafe(),
								posPointY.getOrElse("0.0").toDoubleSafe()
							)

		val transactionPoint = if (terminalPoint.getX > 0.0) terminalPoint else posPoint
		val operationType = if (posPoint.getX > 0) 0 else 1
		val cCurrency = currency.getOrElse("643.0").toDoubleSafe(643.0)
		val amountPower10 = math.pow(10, amount.getOrElse("0.0").toDoubleSafe())
		val mccInt = mcc.getOrElse("0").replace(",","").replace("\"", "").toInt
		val districtApartmensCount = Apartment.
								getDistrictApartmensCount(
									transactionPoint, 
									Consts.scoreRadious
								)
		
		new Transaction(
			id,
			if (amount.isEmpty) None else Some(amount.get.toDoubleSafe()),
			amountPower10,
			atm_address, 
			terminalPoint, 
			city, 
			country,
			cCurrency.toInt,
			customer_id.getOrElse(""),
			mccInt,
			pos_address,
			posPoint, 
			terminal_id,
			getDate(sDate),
			transactionPoint,
			operationType,
			districtApartmensCount
		)
	}

	def toCsv(transactions : RDD[Transaction]) : RDD[String] = {
		transactions.map{
			case t => 
				t.csv().mkString(",")
		}
	}
}

@SerialVersionUID(123L)
class Transaction(
		val id : Long,
		val amount : Option[Double],
		val amountPower10 : Double,
		val atm_address : Option[String],
		val atmPoint : Point,
		val city : Option[String], 
		val country : Option[String],
		val currency : Int, 
		val customer_id : String, 
		val mcc : Int,
		val pos_address : Option[String],
		val posPoint : Point,
		val terminal_id : Option[String],
		val date : Option[DateTime],
		val point : Point,
		val operationType : Int,
		val districtApartmensCount : Int
	) extends Serializable {


	def csv(delimeter : String = ",", quoteSign : String = "\"") : List[String] = {

		List(
			id.toString, 
			amount.getOrElse(0).toString,
			amountPower10.toString,
			quote(atm_address.getOrElse("").replace("\"", "").replace("\\", ""), quoteSign),
			if (atmPoint.getX == 0.0) null else atmPoint.getX.toString,
			if (atmPoint.getY == 0.0) null else atmPoint.getY.toString,
			quote(city.getOrElse(""), quoteSign),
			quote(country.getOrElse(""), quoteSign),
			quote(currency.toString, quoteSign),
			quote(customer_id, quoteSign),
			mcc.toString,
			quote(pos_address.getOrElse("").replace("\"", "").replace("\\", ""), quoteSign),
			if (posPoint.getX == 0.0) null else posPoint.getX.toString,
			if (posPoint.getY == 0.0) null else posPoint.getY.toString,
			quote(terminal_id.getOrElse(""), quoteSign),
			if (date.isEmpty) "" else Transaction.dateFormat.print(date.get),
			point.getX.toString,
			point.getY.toString,
			operationType.toString
		)
	}
	
	def quote(str : String, quoteSign : String) : String = quoteSign + str + quoteSign
	
	override def toString = f"""Transaction([id=$id],[amount=$amount],[atm_address=$atm_address],[atmPoint=$atmPoint],[city=$city],[country=$country],[currency=$currency],[customer_id=$customer_id],[mcc=$mcc],[pos_address=$pos_address],[posPoint=$posPoint],[terminal_id=$terminal_id],[date=$date],[transactionPoint=$point], [operationType=$operationType])"""
}
