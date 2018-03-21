package com.lookuut

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Date
import com.esri.core.geometry._

import org.apache.spark.util.StatCounter
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint

import scala.reflect.runtime.{universe => ru}
import scala.math.Ordering

import org.apache.spark.rdd._

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import org.joda.time.DateTime
import org.joda.time.Days


object TrainTransactionsAnalytics {
	def distance(sPoint : Point, ePoint : Point) : Double = {
		val x = (sPoint.getX - ePoint.getX)
		val y = (sPoint.getY - ePoint.getY)
		math.sqrt(x * x + y * y)
	}

	def squaredDistance(sPoint : Point, ePoint : Point) : Double = {
		(sPoint.getX - ePoint.getX) * (sPoint.getX - ePoint.getX) + (sPoint.getY - ePoint.getY) * (sPoint.getY - ePoint.getY)
	}

	def pointCell (p : Point) : (Int, Int) = {	
		(
			math.ceil(p.getX / (TransactionClassifier.scoreRadious * 2)).toInt, 
			math.ceil(p.getY / (TransactionClassifier.scoreRadious * 2)).toInt
		)
	} 
}

class TrainTransactionsAnalytics(private val sparkSession : SparkSession) extends Serializable {
	import sparkSession.implicits._

	def featurePointIdentify (transactions : RDD[Transaction],
								trainTransactions : RDD[TrainTransaction],
								column : String,
								equalPercent : Double,
								equalPointPercent : Double,
								minEqualPointCount : Int
								) : scala.collection.Map[String, Point] = {
		
		val grouppedPoints = trainTransactions.
						filter(t => t.transaction.transactionPoint.getX > 0).
						map(t => (t.transaction.transactionPoint, t)).
						groupByKey.
						map {
							case (point, values) =>
								val customerToPointMap = values.
									map(t => {
										val mirror = ru.runtimeMirror(t.getClass.getClassLoader)
										val shippedTrainedPoint = ru.typeOf[TrainTransaction].decl(ru.TermName(column)).asTerm
										val im = mirror.reflect(t)
										val shippingPointFieldMirror = im.reflectField(shippedTrainedPoint)

										val point = shippingPointFieldMirror.get.asInstanceOf[Point]

										(t.transaction.customer_id, point)
									}).
									filter(t => t._2.getX > 0). 
									groupBy(t => t._1).
									map {
										case (customer_id, values) => 
											(customer_id, values.head._2)
									}.
									toMap

								if (customerToPointMap.size == 0) {
									(point, 0.0, customerToPointMap)
								} else {
									val points = customerToPointMap.
													zipWithIndex.
													map {
														case ((customerId, point), index) => 
									 						(customerId , DBSCANPoint(index + 1, point.getX, point.getY), point)
													}

									val pointIndexToCustomer = points.map{
										case (customerId, dbScanPoint, point) => 
											(dbScanPoint.id, (customerId, point))
									}.
									toMap

									val minPointsCount = if (points.size > 4) 4 else points.size

									val clusters = DBSCAN2(TransactionClassifier.scoreRadious, minPointsCount).
													cluster(points.map(_._2))

									val pointClusters = clusters.
										map(t => (math.abs(t.clusterID), t)).
										filter(t => t._1 > 0).
										groupBy(_._1).
										map{case (key, values) => (values, values.size)}

									if (pointClusters.size > 0) {
										val maxPointCluster = pointClusters.maxBy(_._2)
										val customers = maxPointCluster._1.map(t => pointIndexToCustomer.get(t._2.id).get)
										val percent = maxPointCluster._2.toDouble / points.size
										(point, percent, customers)
									} else {
										(point, 0.0, customerToPointMap)
									}									
								}
						}.
						filter(t => t._2 >= equalPercent && t._3.size > 0).
						map(t => (t._1, t._3)).
						persist //point <-> list[customers] 

		val customerToPointMap = grouppedPoints.
									map(t => t._2.map(tt => (tt._1, t._1))).
									flatMap(t => t).
									groupBy(_._1).
									collectAsMap
		
		val customerToPointBroadcast = sparkSession.sparkContext.broadcast(customerToPointMap)

		val testPoints = transactions.
			filter(t => t.transactionPoint.getX > 0).
			map(t => (t.transactionPoint, t)).
			groupByKey.
			mapValues {
				case values => 
					values.map(t => t.customer_id).toSet
			}.
			persist

		val definedCustomers = testPoints.
			join(grouppedPoints).
			map {
				case (point, (testCustomersList, trainCustomers)) => 
					testCustomersList.
						map(test_customer_id => (test_customer_id, point, trainCustomers))
			}.
			flatMap(t => t).
			groupBy(_._1).
			map {
				case (test_customer_id, gruppedPoints) =>
					val maxTrainCustomer = gruppedPoints.map{
						case t => 
							t._3.map {
								case (l) => 
									(l._1, l._2)//customer_id, homePoint
							}
					}.
					flatMap(t => t).
					groupBy(_._1).
					map(t => (t._1, t._2.head._2, t._2.size)).
					maxBy(t => t._3)

					val customerId = maxTrainCustomer._1
					val point = maxTrainCustomer._2
					val equalCount = maxTrainCustomer._3
					val trainCustomerPointCount = customerToPointBroadcast.value.get(customerId).size
					val _equalPercent = equalCount.toDouble / trainCustomerPointCount

				(test_customer_id, point, _equalPercent, equalCount)
			}.
			filter(t => t._3 >= equalPointPercent && t._4 >= minEqualPointCount).
			map(t => (t._1, t._2)).
			collectAsMap

		val newTransactions = transactions.filter {
				case (t) => !definedCustomers.contains(t.customer_id)
			}

		val predictedTrainTransactions = transactions.filter{
				case (t) => definedCustomers.contains(t.customer_id)
			}.
			map{
				case (t) => 
					val point = definedCustomers.get(t.customer_id).get
					new TrainTransaction(t, point, point)
			}

		println("=========> calculated customers count " + definedCustomers.size)	
		
		if (definedCustomers.size > 0) {
			
			featurePointIdentify(
				newTransactions.repartition(8), 
				trainTransactions.union(predictedTrainTransactions).repartition(8), 
				column,
				equalPercent,
				equalPointPercent,
				minEqualPointCount
			)
		} else {
			if (column == "homePoint") {
				trainTransactions.
					map(t => (t.transaction.customer_id, t.homePoint)).
					collectAsMap
			} else {
				trainTransactions.
					map(t => (t.transaction.customer_id, t.workPoint)).
					collectAsMap	
			}
		}
	}

	def clusteringStat (trainTransactions : RDD[TrainTransaction])
	{
		val percent = trainTransactions.map(t => (t.transaction.customer_id , t)).
			groupByKey.
			map {
				case (customer_id, transactions) => 
					val homePoint = transactions.head.homePoint
					if (
						transactions.filter(p => 
											TrainTransactionsAnalytics.squaredDistance(
												p.transaction.transactionPoint, homePoint
											) <= TransactionClassifier.squareScoreRadious).
								size > 0
							) 1 else 0
			}.mean

		println("Train transactions who have point around home " + percent)
		println("TrainTransaction count " + trainTransactions.count)
		
		val customers = trainTransactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			map {
				case (customer_id, tTransactions) => 

					val homePoint = tTransactions.head.homePoint
					val minDistance = tTransactions.
						map(
							t => TrainTransactionsAnalytics.squaredDistance(
												t.transaction.transactionPoint , homePoint
											)
						).min

					val customerPointCount = tTransactions.map(_.transaction.transactionPoint).toSet.size
					val transactionsMap = tTransactions.
										map(_.transaction).
										toList.
										sortWith(_.transactionPoint.getX > _.transactionPoint.getX).
										zipWithIndex.
										map{case (t, index) =>  (index, t)}.
										toMap

					val dbScanPoints = transactionsMap.
										map{case (id, t) => 
											DBSCANPoint(
														id, 
														t.transactionPoint.getX, 
														t.transactionPoint.getY
													)
										}
					val cluteredPoints = DBSCAN2(0.02, 1).
											cluster(dbScanPoints)

					val clusters = cluteredPoints.
						map(p => (p.clusterID, transactionsMap.get(p.id.toInt).get)).
						groupBy(_._1).
						map{
							case (clusterId, transactions) =>
								val tCount = transactions.size
								val clusterDates = transactions.map(_._2.date.get).toSeq.sortWith(_.getMillis > _.getMillis).toSet.toList
								val maxClusterDate = clusterDates.head
								val minClusterDate = clusterDates.last

								val duration = math.abs(Days.daysBetween(maxClusterDate, minClusterDate).getDays)
								
								val clusterPoints = transactions.
									map(_._2.transactionPoint).
									toSet.
									toList

								val pointNears = clusterPoints.map{
									p => clusterPoints.filter(pp => 
											TrainTransactionsAnalytics.squaredDistance(
												pp, p
											) <= TransactionClassifier.squareScoreRadious
										).size
								}

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
								val isHomeAround = transactions.filter(t => 
										TrainTransactionsAnalytics.squaredDistance(
											t._2.transactionPoint, homePoint
										) <= minDistance
									).size > 0

								val pointInCluster = transactions.map(_._2.transactionPoint).toSet.size
								val tAtWeekEndCount = transactions.
									map(t => t._2.date.get.getDayOfWeek - 1).
									filter(t => t >= 5 && t <= 6).size

								val tAtWeekdayCount = transactions.
									map(t => t._2.date.get.getDayOfWeek - 1).
									filter(t => t <= 4).size

								val posOperationCount = transactions.map(t => t._2.posPoint.getX > 0).size
								val terminalOperationCount = transactions.map(t => t._2.atmPoint.getX > 0).size

								val avgDistance = {
									if (clusterPoints.size > 1) {
										10 - clusterPoints.map{
											p => clusterPoints.map(pp => 
													TrainTransactionsAnalytics.distance(
														pp, p
													)
												).sum
										}.sum / clusterPoints.size
									} else 0
								}
									
								val aroundHomeMCCCodesCount = transactions.
										filter(t => aroundHomeMCCCodes.contains(t._2.mcc)).size

								val farFromHomeMCCCodesCount = transactions.
										filter(t => farFromHomeMCCCodes.contains(t._2.mcc)).size

								val farFromHomeMccPercent = 1 - farFromHomeMCCCodesCount.toDouble / tCount

								(
									Vectors.dense(
										if (isHomeAround) 1.0 else 0.0, 
										pointInCluster.toDouble,
										duration.toDouble, 
										dateRatio,
										pointNears.sum.toDouble / pointNears.size,
										tAtWeekEndCount.toDouble / tCount,
										tAtWeekdayCount.toDouble / tCount,
										avgDistance,
										aroundHomeMCCCodesCount / tCount,
										farFromHomeMccPercent,
										clusterId
									),
									transactions.map(t=> t._2),
									homePoint
								)
						}

					val customerMaxClusters = (1 to 9).map {
						case feature => 
							val cluster = clusters.maxBy(t => t._1(feature))
						(cluster._1(10), cluster)
					}.
					groupBy(_._1).
					map{case (clusterId, clusters) => clusters.head}

					(customer_id, customerMaxClusters, customerMaxClusters.size)
			}.
			cache

		val customersWithPointAroundHome = customers.map(customer => 
			if(customer._2.map{
				case cluster => 
					val clusterAroundHome = cluster._2._2.map{ 
						case transaction => 
							if (TrainTransactionsAnalytics.
									distance(
										transaction.transactionPoint, cluster._2._3
									) <= TransactionClassifier.scoreRadious
							) 1 else 0
					}.sum

					if (clusterAroundHome >= 1) 1 else 0
			}.sum >= 1) 1 else 0).mean
		
		val customerAVGClusterCount = customers.map(c => c._2.size).mean
		val clustersCount = customers.map(c => c._2.size).sum
		val customersCount = customers.count
		println(f"Unqiue points count ===> ${customersWithPointAroundHome} ${customerAVGClusterCount} ${clustersCount} ${customersCount}")
		/*
		val pointsResult = customers.map{case customer => 
			val points = customer._2.map{
				case cluster => 

					val clusterTransactions = cluster._2._2
					val clusterApartmentsCount = clusterTransactions.
													map(t => t.transactionPoint).
													toSet.
													map((p: Point) => Apart.
															getDistrictApartmens(p, TransactionClassifier.scoreRadious)
													).
													flatMap(t => t).
													toSet.
													map((a: Apart) => a.apartmens).
													sum

					val clusterTransactionsCount = cluster._2._2.size
					val homePoint = cluster._2._3
					clusterTransactions.map{
						case transaction => 
							val point = transaction.transactionPoint
							val cityClusterID = TransactionPointCluster.getPointCluster(point).toDouble
							val countryID = TransactionClassifier.countriesCategories.get(transaction.country.getOrElse("")).get.toDouble
							val currencyID = TransactionClassifier.currencyCategories.get(transaction.currency).get.toDouble
							val mcc = TransactionClassifier.mccCategories.get(transaction.mcc).get.toDouble
							val dayOfWeek = TransactionClassifier.getDateCategory(transaction.date.get).toDouble
							val pointTransactions = clusterTransactions.
														filter(t => t.transactionPoint == transaction.transactionPoint)
										
							val ratio = pointTransactions.
											size.toDouble / clusterTransactionsCount

							val nearsCount = clusterTransactions.map(t => t.transactionPoint).
												toSet.
												filter {
													 (t: Point) => 
														TrainTransactionsAnalytics.
															distance(
																t, point
															) <= TransactionClassifier.scoreRadious
												}.size

							val pointDates = pointTransactions.map(_.date.get).toSeq.sortWith(_.getMillis > _.getMillis).toSet.toList
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
											
							val pointApartmentsCount = Apart.getDistrictApartmensCount(point, TransactionClassifier.scoreRadious)
							val distance = TrainTransactionsAnalytics.distance(point, homePoint)
							val apartmentsPercent = if (clusterApartmentsCount == 0) 0 else transaction.districtApartmensCount.toDouble / clusterApartmentsCount
						(
							Vectors.dense(
								dayOfWeek,
								cityClusterID, 
								countryID,
								mcc,
								dayOfWeek,
								ratio,
								nearsCount,
								dateRatio,
								if (aroundHomeMCCCodes.contains(transaction.mcc)) 1.0 else 0.0,
								if (farFromHomeMCCCodes.contains(transaction.mcc)) 0.0 else 1.0,
								
							),
							distance,
							point,
							if (distance <= 0.02) 1 else 0
						)
					}
			}.flatMap(t => t)
			val minDistancePoint = points.minBy(_._2)._3

			if (points.filter(t => t._4 >= 1).size <= 0) 
				points.filter(t => t._3 == minDistancePoint).map(t => (t._1, t._2, t._3, 1))
			 else 
				points
		}.
		flatMap(t => t)

		val minDistanceSummary = Statistics.colStats(pointsResult.filter(_._4 >= 1).map(_._1))
		println(minDistanceSummary.mean)  // a dense vector containing the mean value for each column
		println(minDistanceSummary.max)  // column-wise variance
		println(minDistanceSummary.min)
		println(minDistanceSummary.count) 


		val badDistanceSummary = Statistics.colStats(pointsResult.filter(_._4 == 0).map(_._1))
		println(badDistanceSummary.mean)  // a dense vector containing the mean value for each column
		println(badDistanceSummary.max)  // column-wise variance
		println(badDistanceSummary.min)
		println(badDistanceSummary.count)
		*/
		
	}


	
	private val aroundHomeMCCCodes = List(3011, 3047, 5169, 5933, 7211)
	private val farFromHomeMCCCodes = List(763,
										 1731,
										 1750,
										 1761,
										 2842,
										 3351,
										 3501,
										 3503,
										 3504,
										 3509,
										 3512,
										 3530,
										 3533,
										 3543,
										 3553,
										 3579,
										 3586,
										 3604,
										 3634,
										 3640,
										 3642,
										 3649,
										 3665,
										 3692,
										 3750,
										 3778,
										 4112,
										 4119,
										 4121,
										 4131,
										 4215,
										 4225,
										 4411,
										 4457,
										 4511,
										 4582,
										 4722,
										 4784,
										 4789,
										 4814,
										 4816,
										 5044,
										 5046,
										 5051,
										 5065,
										 5085,
										 5094,
										 5131,
										 5137,
										 5139,
										 5193,
										 5198,
										 5199,
										 5231,
										 5261,
										 5271,
										 5309,
										 5511,
										 5521,
										 5531,
										 5551,
										 5561,
										 5571,
										 5599,
										 5712,
										 5713,
										 5718,
										 5733,
										 5734,
										 5735,
										 5813,
										 5816,
										 5947,
										 5963,
										 5965,
										 5969,
										 5970,
										 5971,
										 5994,
										 5996,
										 5997,
										 5998,
										 6010,
										 7011,
										 7012,
										 7261,
										 7296,
										 7299,
										 7333,
										 7338,
										 7393,
										 7394,
										 7399,
										 7512,
										 7523,
										 7531,
										 7535,
										 7622,
										 7922,
										 7929,
										 7991,
										 7992,
										 7996,
										 7998,
										 7999,
										 8042,
										 8062,
										 8211,
										 8244,
										 8249,
										 8351,
										 8911,
										 9211,
										 9222,
										 9311,
										 9399)
}