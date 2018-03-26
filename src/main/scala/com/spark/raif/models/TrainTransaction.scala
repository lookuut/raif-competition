package com.spark.raif.models


import com.esri.core.geometry.Point
import org.apache.spark.rdd.RDD
import com.spark.utils.StringUtils._

object TrainTransaction {
	
	val homePoint = "homePoint"
	val workPoint = "workPoint"

	private val paramsMaxCount = 20
	def parse(line : String, id : Long) : TrainTransaction = {

		val parsedRow = Transaction.parseString(line, paramsMaxCount)
		val transcation = Transaction.init(id,
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
			parsedRow(8).getOrElse("0.0").toDoubleSafe(),
			parsedRow(9).getOrElse("0.0").toDoubleSafe()
		)
		
		val workPoint = new Point(
			parsedRow(16).getOrElse("0.0").toDoubleSafe(),
			parsedRow(17).getOrElse("0.0").toDoubleSafe()
		)
		
		new TrainTransaction(
			transcation,
			workPoint,
			homePoint
		)
	}

	def toCsv(trainTransactions : RDD[TrainTransaction]) : RDD[String] = {
		trainTransactions.map{
			case t => 
				(
					t.transaction.csv() 
					++ 
					List(
						t.homePoint.getX.toString,
						t.homePoint.getY.toString,
						if (t.workPoint.getX == 0.0) null else t.workPoint.getX.toString,
						if (t.workPoint.getY == 0.0) null else t.workPoint.getY.toString
					)
				).
				mkString(",")
		}
	}	

}

@SerialVersionUID(1234L)
class TrainTransaction(
		val transaction : Transaction,
		val workPoint : Point,
		val homePoint : Point
	) extends Serializable {

	def getTargetPoint(column : String) : Point = {
		if (column == TrainTransaction.homePoint) 
			homePoint 
		else 
			workPoint
	}

	override def toString = f"""TrainTransaction([homePoint=$homePoint],[workPoint=$workPoint],[Transaction=$transaction])"""
}
