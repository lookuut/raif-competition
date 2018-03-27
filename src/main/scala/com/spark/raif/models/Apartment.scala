/**
 *
 *
 * @author Lookuut Struchkov
 * @desc object for parse Russia apartments csv file from https://www.reformagkh.ru/
 * 		 Save parsed files to Map, spatial index as key of map
 *
 */

package com.spark.raif.models

import com.esri.core.geometry.Point
import com.spark.utils.parser.CSV

import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import com.spark.utils.StringUtils._
import com.spark.utils.geometry.PointUtils._

object Apartment {
	private val paramsCount = 10

	def parse(line : String, id : Long) : Apartment = {

		val row = CSV.parseString(line, ',', '"', '\\')
		
		val parsedRow = (0 until paramsCount).map(i => {
			if (i < row.size) Some(row(i).trim) else None
		})

		val pointX = parsedRow(0)
		val pointY = parsedRow(1)
		val strYear = parsedRow(2)
		val floors = parsedRow(3).getOrElse("1..1").split("\\.\\.")

		val floor = if(floors.size >= 2) floors.last.toInt else 1
		val strApartmens = parsedRow(4)
		val strParking = parsedRow(5).getOrElse("0")

		val point = new Point(
								pointX.getOrElse("0.0").toDoubleSafe() , 
								pointX.getOrElse("0.0").toDoubleSafe()
							)
		val parking = if (strParking.isInteger) strParking.toInt else 0

		val apartmens = if (strApartmens.isEmpty || strApartmens.get == "") 1 else strApartmens.get.toInt
		val year = if (strYear.isEmpty || strYear.get == "") None else Some(strYear.get.toInt)
		new Apartment(
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

	val apartmensSpatial = HashMap.empty[Double, Map[(Int, Int), Map[Point, Apartment]]]

	def init(apartmens : RDD[Apartment], radious : Double) {
	
		val radiousSpatialIndex = apartmens.
			map(a => (Apartment.spatialIndex(a.point, radious), a)).
			groupByKey.
			mapValues {
				case aparts => 
					aparts.map(a => (a.point, a)).toMap
			}.
			collectAsMap
		
		apartmensSpatial += (radious -> radiousSpatialIndex.toMap)
	}
	
	def addSpatialIndex(apartmens : RDD[Apartment], radious : Double) {
		if (!apartmensSpatial.contains(radious)) {
			init(apartmens, radious)
		}
	} 

	def getDistrictApartmens(point : Point, radious : Double) : List[Apartment] = {
		if (!apartmensSpatial.contains(radious)) {
			return List()
		}

		val xAxis = (point.getX / radious).floor.toInt
	    val yAxis = (point.getY / radious).floor.toInt

		(xAxis - 1 to xAxis + 1).flatMap(i =>
			(yAxis - 1 to yAxis + 1).flatMap(j =>
				apartmensSpatial.get(radious).get.
					getOrElse((i, j), Map.empty).
					filter{case (p , apartmens) => point.inRadios(p, radious)}.
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
class Apartment(
		val id : Long,
		val point : Point,
		val year : Option[Int],
		val floor : Int,
		val apartmens : Int,
		val parking : Int
	) extends Serializable {

	override def toString = f"""Transaction([id=$id],[point=$point],[year=$year],[point=$point],[floor=$floor],[apartmens=$apartmens],[parking=$parking])"""
}
