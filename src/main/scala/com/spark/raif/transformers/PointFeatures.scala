/**
 *
 *
 * @author Lookuut Struchkov
 * @desc Transform transaction to vector of features used models
 * 	
 *
 */
 
package com.spark.raif.transformers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

import com.esri.core.geometry.Point
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint

import org.joda.time.Days
import org.joda.time.DateTime

import com.spark.raif.models._
import com.spark.utils.geometry.PointUtils._

import com.spark.raif.Consts
import com.spark.raif.transformers.{PointFeatures => PF}


object PointFeatures {

	def pointCell (p : Point) : (Int, Int) = {	
		(
			math.ceil(p.getX / (Consts.scoreRadious * 2)).toInt, 
			math.ceil(p.getY / (Consts.scoreRadious * 2)).toInt
		)
	}

	val minDistanceThreshold = Consts.scoreRadious * 3 
}

class PointFeatures (
		private val sparkSession : SparkSession, 
		private val categoricalFeatures : CategoricalFeatures
	) extends Serializable {

	private val weekOfYear = (0 to 52).toList
	private val weekdayVect = (0 to 6).toList
	private val yearHolidaysVec = (0 to 7).toList

	def transactionDateVector(transactions : Iterable[Transaction]) : Array[Double] = {
		val holidays = transactions.map{
			case t => 
				t.date.get.dayOfYear.get match {
					case 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 => 0
					case 53 | 54 | 55 | 56 | 57 => 1
					case 67 => 2
					case 118 | 119 | 120 | 121 => 3
					case 125 | 126 | 127 | 128 | 129  => 4
					case 160 | 161 | 162 | 163 => 5
					case 307 | 308 | 309 | 310 => 6
					case _ => -1
				} 
		}.
		groupBy(t => t).
		mapValues(_.size).
		toMap

		val holidaysVec = (
			for(key <- yearHolidaysVec)
    			yield holidays.getOrElse(key, 0).toDouble
    	).toArray

		val weekdayCount = transactions.
			map(t => t.date.get.getDayOfWeek - 1).
			groupBy(t => t).
			mapValues(_.size).toMap

		val weekDayVec = (
			for(key <- weekdayVect)
    			yield weekdayCount.getOrElse(key, 0).toDouble
    	).toArray

    	holidaysVec ++ weekDayVec
	}

	def mccVector(transactions : Iterable[Transaction]) : Array[Double] = {
		val tMccIndex = transactions.
			map(t => (categoricalFeatures.getMccCategories.get(t.mcc).get, 1)).
			toMap

		categoricalFeatures.getMccCategories.
			keys.
			toList.
			map(t => if (tMccIndex contains t) 1.0 else 0.0).
			toArray
	}

	def pointFeature(
						point: Point,
						pointTransactions : Iterable[Transaction], 
						clusterTransactions : Iterable[Transaction],
						clusterApartmentsCount : Int,
						clusterTransactionsCount : Int
					) : Vector = {

		val nearPointTransactions = clusterTransactions.
										filter(t => point.inRadios(t.point, Consts.scoreRadious))

		val pointNearsWeight = Shops.getPointWeight(point, nearPointTransactions.
									map(t => t.mcc).
									toSet 
								)

		val currencyIndex = categoricalFeatures.getCurrencyCategories.
								get(pointTransactions.head.currency).
								get
		
		val currencyVec = categoricalFeatures.getCurrencyCategories.
							values.
							map(t => if (currencyIndex == t) 1.0 else 0.0).
							toArray

		val ratio = pointTransactions.
						size.toDouble / clusterTransactionsCount

		val nearsCount = nearPointTransactions.
							map(t => t.point).
							toSet.
							size

		val pointDates = pointTransactions.
							map(_.date.get).
							toSeq.
							sortWith(_.getMillis > _.getMillis).
							toSet.toList
		val dateRatio = {
							if (pointDates.size >= 2) {
								pointDates.
									sliding(2).
									toList.
									map{
										case t => 
											math.abs(Days.daysBetween(t(0), t(1)).getDays)
									}.
									sum / pointTransactions.size
							} else {
								1.0
							}
						}

		val duration = math.abs(Days.daysBetween(pointDates.head, pointDates.last).getDays)
		val pointApartmentsCount = Apartment.getDistrictApartmensCount(point, Consts.scoreRadious)
		val apartmentsPercent = if (clusterApartmentsCount == 0) 0 else pointTransactions.head.districtApartmensCount.toDouble / clusterApartmentsCount
		val density = clusterTransactions.map{
									case t => 
										clusterTransactions.map(tt => tt.point.distance(t.point)).sum
								}.sum / nearsCount

		Vectors.dense(
			Array(
				ratio,
				density,
				nearsCount,
				apartmentsPercent,
				dateRatio,
				duration,
				pointNearsWeight
			)
			++ 
			transactionDateVector(pointTransactions)
			++
			mccVector(pointTransactions)
			++
			currencyVec
		)
	}
	
	def clusterFeature(transactions : Iterable[Transaction]) : Vector = {

		val countryIndex = categoricalFeatures.getCountriesCategories.
								get(transactions.head.country.getOrElse("")).get
		
		val countryVec = categoricalFeatures.getCountriesCategories.
							values.
							toList.
							map(t => if (countryIndex == t) 1.0 else 0.0).
							toArray
	
		val tCount = transactions.size
		val clusterDates = transactions.map(_.date.get).toSeq.sortWith(_.getMillis > _.getMillis).toSet.toList
		val maxClusterDate = clusterDates.head
		val minClusterDate = clusterDates.last

		val duration = math.abs(Days.daysBetween(maxClusterDate, minClusterDate).getDays)
		
		val clusterPoints = transactions.
			map(_.point).
			toSet.
			toList

		val pointNears = clusterPoints.
			map(
				p => clusterPoints.filter(pp => 
					pp.inRadios(p, Consts.scoreRadious)
				).size
			)

		val dateRatio = {
			if (clusterDates.size >= 2) {
				clusterDates.
					sliding(2).
					toList.
					map{
						case t => 
							math.abs(Days.daysBetween(t(0), t(1)).getDays)
					}.
					sum / tCount
			} else {
				1.0
			}
		}
		
		val pointInCluster = transactions.map(_.point).toSet.size
		val tAtWeekEndCount = transactions.
			map(t => t.date.get.getDayOfWeek - 1).
			filter(t => t >= 5 && t <= 6).size

		val tAtWeekdayCount = transactions.
			map(t => t.date.get.getDayOfWeek - 1).
			filter(t => t <= 4).size

		val avgDistance = {
			if (clusterPoints.size > 1) {
				10 - clusterPoints.map{
					p => clusterPoints.map(pp => 
							pp.distance(p)
						).sum
				}.sum / clusterPoints.size
			} else 0
		}
		
		Vectors.dense(
			Array(
				pointInCluster.toDouble,
				duration.toDouble, 
				dateRatio,
				pointNears.sum.toDouble / pointNears.size,
				tAtWeekEndCount.toDouble / tCount,
				tAtWeekdayCount.toDouble / tCount,
				avgDistance
			) 
			++
			countryVec
		)
	}

	def clusteringCustomerTransactions(transactions : Iterable[Transaction]) : Map[Int, Iterable[(Int, Transaction)]] = {
		val transactionsMap = transactions.
								toList.
								sortWith(_.point.getX > _.point.getX).
								zipWithIndex.
								map{case (t, index) =>  (index, t)}.
								toMap

		val dbScanPoints = transactionsMap.
							map{case (id, t) => 
								DBSCANPoint(
											id, 
											t.point.getX, 
											t.point.getY
										)
							}

		DBSCAN2(Consts.scoreRadious, 1).
			cluster(dbScanPoints).
			map(p => (p.clusterID, transactionsMap.get(p.id.toInt).get)).
			groupBy(_._1)
	}
	
	def clusterSelector(customer_id : String, 
						clusters : Iterable[(Vector, Iterable[(Vector, Double, Point)], Double, Int)] ) : 
						(String, Iterable[(Vector, Iterable[(Vector, Double, Point)], Double, Int)]) = {

		(customer_id, clusters)
	}
	
	def buildClusterFeature(
							transactions : Iterable[Transaction], 
							clusterId : Int, 
							targetPoint : Option[Point], 
							minDistance : Double) : (Vector, Iterable[(Vector, Double, Point)], Double, Int) = {

		val tCount = transactions.size

		val isHomeAround = if (!targetPoint.isEmpty) {
								transactions.filter(t => 
									t.point.inRadios(targetPoint.get, minDistance)
								).size > 0
							} else {
								false
							}
								
		
		val clusterApartmentsCount = transactions.
							map(t => t.point).
							toSet.
							map((p: Point) => Apartment.
									getDistrictApartmens(p, Consts.scoreRadious)
							).
							flatMap(t => t).
							toSet.
							map((a: Apartment) => a.apartmens).
							sum

		val feature = clusterFeature(transactions)
		(
			feature,
			transactions.
				map(t => (t.point ,t)).
				groupBy(_._1).
				map{
					case (point, pointTransactions) =>
						(
							pointFeature(
								point, 
								pointTransactions.map(_._2), 
								transactions, 
								clusterApartmentsCount, 
								tCount
							),
							(if (!targetPoint.isEmpty && point.inRadios(targetPoint.get, minDistance)) 1.0 else 0.0),
							point
						)
				}
			,
			if (isHomeAround) 1.0 else 0.0,
			clusterId
		)
	} 

	def expandData (transactions : 
						RDD[(String, 
							Iterable[(Vector, 
								Iterable[(Vector, Double, Point)], Double, Int)])] 
					) : RDD[(String, Point, Double, Vector)] = {
		transactions.
			flatMap(customer => 
				customer._2.map(
					cluster => 
						cluster._2.map{
							case point => 
								(
									customer._1, //customer_id
									point._3, //point
									point._2,  //is_home/is_work
									( 
										Vectors.dense(cluster._1.toArray ++ point._1.toArray) //point feature
									)
								)
						}
				).flatMap(t => t)
		)
	}
	
	def filter(tTransactions : RDD[(Transaction, Option[Point])]) : RDD[(Transaction, Option[Point])] = {
		tTransactions.
			map(t => (t._1.customer_id, (t._1, t._2))).
			groupByKey.
			map{
				case (customer_id, transactions) =>
					val trashCond = 
						transactions.
						filter(t => 
								t._2.isEmpty || t._1.point.inRadios(t._2.get, PointFeatures.minDistanceThreshold)
						).size > 0
					(customer_id, transactions, trashCond)
			}.
			filter(_._3).
			flatMap(t => t._2)
	}

	def prepareTransactions (transactions : RDD[(Transaction, Option[Point])]) 
		: 
		RDD[(String, Point, Double, Vector)] = //customer_id, point, label, features
	{	

		val customers = filter(transactions).
			map(t => (t._1.customer_id, (t._1, t._2))).
			groupByKey.
			map {
				case (customer_id, tTransactions) => 
					val transactions = tTransactions.map(t => t._1)
					val targetPoint = tTransactions.head._2
					val minDistance = {
						if (!targetPoint.isEmpty) {
							val minDistances = transactions.
								map(
									t => t.point.squareDistance(targetPoint.get)
								).
								filter(_ <= PF.minDistanceThreshold)

							val minDist = if (minDistances.size > 0) minDistances.min else PF.minDistanceThreshold
							math.max(minDist, Consts.scoreRadious)
						} else {
							0
						}
					}

					val clusters = clusteringCustomerTransactions(transactions).
						map{
							case (clusterId, transactions) =>
								buildClusterFeature(transactions.map(_._2), clusterId, targetPoint, minDistance)
						}

					clusterSelector(customer_id, clusters)
			}

		expandData(customers)
	}
}