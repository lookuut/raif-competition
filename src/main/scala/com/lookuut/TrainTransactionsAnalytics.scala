package com.lookuut

import java.nio.file.{Paths, Files}
import java.util.Calendar
import java.time.LocalDateTime

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Date
import com.esri.core.geometry._

import org.apache.spark.util.StatCounter

import scala.collection.mutable.ListBuffer
import org.joda.time.DateTimeConstants
import org.apache.spark.storage.StorageLevel
import scala.util.{Try,Success,Failure}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint
import java.io._
import scala.reflect.runtime.{universe => ru}
import scala.math.Ordering

import org.apache.spark.rdd._


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

	def districts (conf : SparkConf, 
						sparkContext : SparkContext, 
						sqlContext : SQLContext,
						trainTransactions : RDD[TrainTransaction]
					) {

		val file = "/home/lookuut/Projects/raif-competition/resource/transaction-point-clusters-2/part-00000"
		val points = sparkContext.objectFile[(Point, Set[Point])](file)

		val clusterCount = points.count
		val oneMorePointClusterCount = points.filter(t => t._2.size > 1).count

		val keyPoints = points.filter(t => t._2.size >= 0).map(t => t._1).collect
		val homePointInCircleMean = trainTransactions.map{
			case t => 
				
				if (
					keyPoints.filter{
						case p => 	
							squaredDistance(t.homePoint, p) <= TransactionClassifier.squareScoreRadious
					}.size >= 1
				) 1 else 0
			}.mean

		val workPointInCircleMean = trainTransactions.map{
			case t => 
				
				if (
					keyPoints.filter{
						case p => 	
							squaredDistance(t.workPoint, p) <= TransactionClassifier.squareScoreRadious
					}.size >= 1
				) 1 else 0
			}.mean
				

		println(f"$clusterCount $oneMorePointClusterCount $homePointInCircleMean $workPointInCircleMean")
	} 

	def pointsDistrict(conf : SparkConf, 
						sparkContext : SparkContext, 
						sqlContext : SQLContext, 
						transactions : RDD[Transaction],
						trainTransactions : RDD[TrainTransaction]) {

		
		var points = transactions.
			map(t => t.transactionPoint).
			union(
				trainTransactions.map(t => t.transaction.transactionPoint)
			).
			distinct.
			map(p => (pointCell(p), p)).
			groupByKey.
			repartition(8).
			cache
		
		var broadcastPoints = points.collectAsMap
		var clusters = scala.collection.mutable.HashMap[Point, Set[Point]]()
		var clusterID = 0
		try {			
			while (points.count > 1) {
				val result = getMaxNearPoint(sparkContext, points, clusters, broadcastPoints, clusterID)
				points = result._1
				broadcastPoints = result._2
				val pointsCount = clusters.map(t => t._2.size).sum
				clusterID += 1
				println(f"=======>rest points count ${pointsCount} clusterID $clusterID")
			}
			clusters += (points.first._2.head -> points.first._2.toSet)
		} catch {
			case e : Exception => println("========>>>>>>>>>>>>" + e.getStackTrace.mkString("\n"))
		}
		println(f"====> End clusters count ${clusters.size}")		
		sparkContext.parallelize(clusters.toSeq).coalesce(1).saveAsObjectFile("/home/lookuut/Projects/raif-competition/resource/transaction-point-clusters-2")
	}

	def getMaxNearPoint (sparkContext : SparkContext, 
							points : RDD[((Int, Int), Iterable[Point])], 
							clusteredPoints : scala.collection.mutable.HashMap[Point, Set[Point]], 
							broadcastPoints : scala.collection.Map[(Int, Int), Iterable[Point]],
							clusterID : Int
						) 
						: 
						(
							RDD[((Int, Int), Iterable[Point])] , 
							scala.collection.Map[(Int, Int), Iterable[Point]]
						) = {

		val maxNearPoint = points.
			map{
					case ((x, y), _points) => 
							_points.map {
								case point =>
									val buffer = ListBuffer[Point]()
									
									for (i <- -1 to 1) {
										for (j <- -1 to 1) {
											if (broadcastPoints.contains((x + i, y + j)) && 
												broadcastPoints.get((x + i, y + j)).get.size > 0) {
												buffer ++= broadcastPoints.get((x + i, y + j)).get.
													filter(
														p => 
															squaredDistance(point, p) <= 4 * TransactionClassifier.squareScoreRadious
														).toList
											}
										}
									}
								
								(
									point, 
									buffer.size,
									buffer.toSet
								)
							}
			}.
			flatMap(t => t).
			top(1)(Ordering.by[(Point, Int, Set[Point]), Int](_._2))

		val point = maxNearPoint(0)._1
		val clusteredPointSet = maxNearPoint(0)._3
		val pointCells = clusteredPointSet.map(p => pointCell(p)).toSet
		
		val filteredPoints = {
				val rddPoints = points.
					map {
						case (cell, points) => 
							if (pointCells.contains(cell)) {
								(cell, points.filter(p => !clusteredPointSet.contains(p)))		
							}  else {
								(cell, points)		
							}
					}

				if (clusterID % 11 == 0) {
					sparkContext.parallelize(rddPoints.collect)
				} else {
					rddPoints
				}				
		}
			

		val filteredBroadcastPoints = {
				val rddPoints = broadcastPoints.
								map {
									case (cell, points) => 
										if (pointCells.contains(cell)) {
											(cell, points.filter(p => !clusteredPointSet.contains(p)))		
										}  else {
											(cell, points)		
										}
								}

				if (clusterID % 11 == 0) {
					rddPoints.toMap
				} else {
					rddPoints
				}				
		}

		clusteredPoints += (point -> clusteredPointSet)

		(
			filteredPoints, 
			filteredBroadcastPoints
		)
	}

	def featurePointIdentify (conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext, transactions : RDD[Transaction],
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
		
		val customerToPointBroadcast = sparkContext.broadcast(customerToPointMap)

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
				conf, 
				sparkContext, 
				sqlContext, 
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

	def extendByMaxNearPoints (
		conf : SparkConf, 
						sparkContext : SparkContext, 
						sqlContext : SQLContext, 
						transactions : RDD[Transaction],
						trainTransactions : RDD[TrainTransaction], 
						column : String,
						identityPoints : scala.collection.Map[String, Point]) : scala.collection.Map[String, Point] =
	{
		transactions.
			filter(t => t.transactionPoint.getX > 0).
			map(t => (t.customer_id , t)).
			groupByKey.
			mapValues{
				case (values) =>
					val seq = values.toSeq
					
					val maxPoints = values.map{
						case transaction => 
							seq.map {
								case t => 
									val x = t.transactionPoint.getX - transaction.transactionPoint.getX
									val y = t.transactionPoint.getY - transaction.transactionPoint.getY
									val distance = math.sqrt(x * x + y * y)
									(transaction.transactionPoint, transaction.id, t.id , distance)
							}
					}.
					flatMap(t => t).
					filter(t => t._2 != t._3 &&  t._4 <= TransactionClassifier.scoreRadious).
					map(t => (t._1, t._3)).
					groupBy(_._1).
					map {
						case (point, points) => 
							(point, points.size)
					}.
					toSeq.
					sortWith(_._2 > _._2).
					take(2)

					if (maxPoints.size > 0) {
						if (column == "homePoint") {
							maxPoints(0)._1
						}  else {
							if (maxPoints.size >= 2) maxPoints(1)._1 else maxPoints(0)._1
						}
					} else {
						values.head.transactionPoint
					}
					
			}.
			collectAsMap.
			map{
				case (customerId, point) => 
					(customerId, identityPoints.getOrElse(customerId, point))
			}
	}


	def equalPointsTransactions (conf : SparkConf, 
									sparkContext : SparkContext, 
									sqlContext : SQLContext, 
									transactions : RDD[Transaction],
									trainTransactions : RDD[TrainTransaction]) {

		val grouppedPoints = trainTransactions.
						filter(t => t.transaction.transactionPoint.getX > 0).
						map(t => (t.transaction.transactionPoint, t.transaction.customer_id)).
						groupByKey.
						cache

		val customersRealtions = grouppedPoints.
						map {
							case (key, values) => 
								var buffer = ListBuffer[Seq[String]]()
								val seq  = values.toStream.distinct.toSeq

								for (i <- 0 to seq.size - 1) {
									for (j <- 0 to seq.size - 1) {
										buffer += Seq(seq(i), seq(j))
									}
								}

								buffer.toList.map {
									case(t) =>
										(t(0), t(1), key)
								}.
								filter(t => t._1 != t._2)
						}.
						flatMap(t => t).
						groupBy(t => t._1).
						map {
							case (customer1, values) =>
								val customer2Map = values.
									map(t => (t._2, t._3)).
									groupBy(t => t._1).
									map{
										case (t) => 
											val pointsMap = t._2.
												map(tt => (tt._2, 1)).
												toMap
											(t._1, pointsMap)
									}.
									toMap
								(customer1, customer2Map)
						}.
						collectAsMap

		val pointMap = grouppedPoints.
						map {
							case (key, values) => 
								(key, values.toStream.distinct.toSeq.size)
						}.
						collectAsMap


		val customers = trainTransactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			mapValues {
				case (values) => 
					val homePoint = values.head.homePoint
					val uniquePointsCount = values.
												map(t => t.transaction.transactionPoint).
												toStream.
												distinct.
												size
					(homePoint, uniquePointsCount)
			}.collectAsMap

		val results = for (nearsCount <- 2 to 12) yield {
			val result = customersRealtions.map {
				case (customer1, nearCustomers) => 
					nearCustomers.map {
						case (customer2, equalPointMap) =>
							val equalCount = equalPointMap.filter(p => pointMap.get(p._1).get <= nearsCount).size
							val customer1PointsCount = customers.get(customer1).get._2
							val customer2PointsCount = customers.get(customer2).get._2

							val minPointsCount = math.min(customer1PointsCount, customer2PointsCount)
							val percent = equalCount.toDouble / minPointsCount
							val x = customers.get(customer1).get._1.getX - customers.get(customer2).get._1.getX
							val y = customers.get(customer1).get._1.getY - customers.get(customer2).get._1.getY
							val distance = math.sqrt(x * x + y * y)
							(customer1PointsCount, customer2PointsCount, equalCount, distance, percent)
					}
			}.
			flatMap(t => t)//.

			for (percent <- Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)) yield {
				val equalizeCount = result.
										filter(t => (t._5 >= percent.toDouble / 100)).
										size / 2
				val homeEqualCount = result.
										filter(t => t._5 >= percent.toDouble / 100 && t._4 <= TransactionClassifier.scoreRadious).
										size / 2

				(percent, equalizeCount, homeEqualCount, homeEqualCount.toDouble / equalizeCount, nearsCount)
			}
		}

		results.flatMap(t=> t).sortBy(_._5).foreach(println)
	} 

	def homeWorkPointPercent (
			conf : SparkConf, 
			sparkContext : SparkContext, 
			sqlContext : SQLContext, 
			trainTransactions : RDD[TrainTransaction])
	{
		import sqlContext.implicits._
		
		val customerTransactions = trainTransactions.
			filter(t => t.transaction.transactionPoint.getX > 0).
			cache
		
		val customerCount = customerTransactions.map(_.transaction.customer_id).countByValue.size
		val transactionCount = trainTransactions.count
		val transactionPointCount = trainTransactions.count
		val pointPercent = transactionPointCount.toDouble / transactionCount
		println(f"=====> $transactionCount $transactionPointCount $pointPercent")

		val t = customerTransactions.map{
			case (t) => 
				val x = t.homePoint.getX - t.transaction.transactionPoint.getX
				val y = t.homePoint.getY - t.transaction.transactionPoint.getY
				(t, math.sqrt(x * x + y * y))
		}.
		map(t => (t._1.transaction.customer_id, (t._1, t._2))).
		groupByKey.
		mapValues {
			case (t) => 
				val count = t.size
				val nearTrans = t.filter(_._2 <= TransactionClassifier.scoreRadious)
				val nearTransCount = nearTrans.size
				

				val nearTransAVGCheck = nearTrans.map(_._1.transaction.amount.get).
											sum / nearTransCount

				val farTransCount = t.filter(_._2 > TransactionClassifier.scoreRadious).size
				val farTransAVGCheck = t.filter(_._2 > TransactionClassifier.scoreRadious).
											map(_._1.transaction.amount.get).sum / farTransCount

				(count, nearTransCount, nearTransCount.toDouble / count, nearTransAVGCheck, farTransAVGCheck)
		}.
		cache

		val percent = t.filter(t => t._2._4 < t._2._5).count.toDouble / customerCount
		
		val percent50 = t.filter(_._2._3 >= 0.5).count.toDouble / customerCount
		val percent40 = t.filter(_._2._3 >= 0.4).count.toDouble / customerCount
		val percent30 = t.filter(_._2._3 >= 0.3).count.toDouble / customerCount
		val percent20 = t.filter(_._2._3 >= 0.2).count.toDouble / customerCount
		val percent10 = t.filter(_._2._3 >= 0.1).count.toDouble / customerCount
		val percent09 = t.filter(_._2._3 >= 0.09).count.toDouble / customerCount
		val percent05 = t.filter(_._2._3 >= 0.05).count.toDouble / customerCount
	}
}