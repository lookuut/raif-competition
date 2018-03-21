package com.lookuut

import com.esri.core.geometry.Point
import org.joda.time.DateTime
import com.lookuut.utils.CSVParser
import org.apache.spark.SparkContext
import scala.collection.Map
import org.apache.spark.rdd._
import com.lookuut.{TrainTransactionsAnalytics => TTA}
import org.apache.spark.sql.SparkSession

object Apart {

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

	def parseString (line : String) : scala.collection.immutable.IndexedSeq[Option[String]] = {
		val row = line.split(",")
		
		val parsedRow = (0 until 8).map(i => {
			if (i < row.size) Some(row(i).trim) else None
		})

		parsedRow
	} 

	def parse(line : String, index : Long) : Apart = {
		val parsedRow = parseString(line)
		build(index, 
			parsedRow(0),
			parsedRow(1),
			parsedRow(2),
			parsedRow(3),
			parsedRow(4),
			parsedRow(5)
		)
	}

	def build (id : Long,
		pointX : Option[String],
		pointY : Option[String],
		strYear : Option[String],
		strFloor : Option[String],
		strApartmens : Option[String],
		strParking : Option[String]
	) : Apart = {

		val point = new Point(
								stringToDouble(pointX).getOrElse(0.0), 
								stringToDouble(pointY).getOrElse(0.0)
							)
		val floor = {
			try {
				strFloor.getOrElse("1..1").split("..").last.toInt
			} catch {
				case e : NumberFormatException => 1
				case e : java.util.NoSuchElementException => 1
			}
		} 

		val parking = {
			try {
				strParking.getOrElse("0").toInt
			} catch {
				case e : NumberFormatException => 0
			}
		}

		val apartmens = if (strApartmens.isEmpty || strApartmens.get == "") 1 else strApartmens.get.toInt
		val year = if (strYear.isEmpty || strYear.get == "") None else Some(strYear.get.toInt)
		new Apart(
			id,
			point,
			year,
			floor,
			apartmens,
			parking
		)
	}

	def spatialIndex(point : Point, radious : Double) : (Int, Int) = {
		((point.getX / radious).floor.toInt, (point.getY / radious).floor.toInt)
	}

	var apartmensSpatial = scala.collection.mutable.HashMap.empty[Double, Map[(Int, Int), Map[Point, Apart]]]
	private val apartmensDataFile = "/home/lookuut/Projects/raif-competition/resource/ru-apartmens.csv"

	def init(radious : Double) {
		val apartmens = BankTransactions.spark.sparkContext.
							textFile(apartmensDataFile).
							filter(!_.contains("latitude,longitude,year")).
							zipWithIndex.
							map{
							case (line, index) => 
								Apart.parse(line, index)
							}

		apartmensSpatial += (
								radious
								-> 
								apartmens.
									map(a => (Apart.spatialIndex(a.point, radious), a)).
									groupByKey.
									mapValues {
										case aparts => 
											aparts.map(a =>  (a.point, a)).toMap
									}.
									collectAsMap
							)	
	}
	
	def addSpatialIndex(radious : Double) {
		if (!apartmensSpatial.contains(radious)) {
			init(radious)
		}
	} 

	def getDistrictApartmens(point : Point, radious : Double) : List[Apart] = {
		if (!apartmensSpatial.contains(radious)) {
			init(radious)
		}

		val xAxis = (point.getX / radious).floor.toInt
	    val yAxis = (point.getY / radious).floor.toInt

	    val xmin = point.getX - radious
	    val ymin = point.getY - radious
	    val xmax = point.getX + radious
	    val ymax = point.getY + radious

		(xAxis - 1 to xAxis + 1).flatMap(i =>
			(yAxis - 1 to yAxis + 1).flatMap(j =>
				apartmensSpatial.get(radious).get.
					getOrElse((i, j), Map.empty).
					filter{case (p , apartmens) => TTA.distance(p, point) <= radious}.
					map(t => t._2).toList
			)
		).toList
	}

	def getDistrictApartmensCount(point : Point, radious : Double) : Int = {
		getDistrictApartmens(point, radious).
			map(t => t.apartmens).
			sum
	}
}

@SerialVersionUID(123L)
class Apart(
		val id : Long,
		val point : Point,
		val year : Option[Int],
		val floor : Int,
		val apartmens : Int,
		val parking : Int
	) extends Serializable {

	override def toString = f"""Transaction([id=$id],[point=$point],[year=$year],[point=$point],[floor=$floor],[apartmens=$apartmens],[parking=$parking])"""
}
