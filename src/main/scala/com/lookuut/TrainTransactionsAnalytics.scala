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

import org.apache.spark.rdd._




object TrainTransactionsAnalytics {

	def distance(sPoint : Point, ePoint : Point) : Double = {
		val x = (sPoint.getX - ePoint.getX)
		val y = (sPoint.getY - ePoint.getY)
		math.sqrt(x * x + y * y)
	}
	
	def minPointRelate (conf : SparkConf, 
						sparkContext : SparkContext, 
						sqlContext : SQLContext, 
						transactions : RDD[Transaction],
						trainTransactions : RDD[TrainTransaction]) 
	{

		val customersTrans = sparkContext.broadcast(
								trainTransactions.
									map(t => (t.transaction.customer_id, t)).
									groupByKey.
									collectAsMap)

		val shopsStat = trainTransactions.
			map(t => (t.transaction.transactionPoint, t)).
			groupByKey.
			map {
				case (shopPoint, tt) =>
					val pointCustomers = tt.
											map(t => (t.transaction.customer_id, t.homePoint)).
											toMap

					val distances = pointCustomers.
						map{
							case (customerId, __homePoint) => 

								val points = customersTrans.value.get(customerId).get.
									zipWithIndex.
									map{
										case (t, index) => 
											DBSCANPoint(index + 1, t.transaction.transactionPoint.getX, t.transaction.transactionPoint.getY)
									}

								val clusters = DBSCAN2(TransactionClassifier.scoreRadious, 1).
													cluster(points).
													filter(t => t.clusterID != 0).
													map(t => (new Point(t.x, t.y), t.globalID)).
													toMap
								if (clusters.size == 0) {
									(customerId, 0.0)
								} else {
									val shopPointCluster = clusters.get(shopPoint).get
									
									val shopClusterPoints = clusters.
										filter{case(point, cluster) => cluster == shopPointCluster}.
										map {
											case(point, cluster) => 
												TrainTransactionsAnalytics.distance(point, shopPoint)
										}

									val min = if (shopClusterPoints.size > 0) shopClusterPoints.maxBy(t => t) else 0
									(customerId, min)	
								}
								
						}.filter(_._2 > 0)

					val _distance = if (distances.size > 0) distances.maxBy(_._2)._2 else 0
					val dist = if (_distance < 0.02) 0.02 else _distance

					val inCirleCustomers = pointCustomers.filter{
							case (cId, homePoint) => 
								TrainTransactionsAnalytics.distance(homePoint, shopPoint) <= dist
						}.
						size

					val customersCount = pointCustomers.
						size

					(shopPoint, inCirleCustomers.toDouble / customersCount, inCirleCustomers, customersCount,  dist)
			}.
			filter(t => t._5 > 0).
			filter(t => t._4 > 2).
			cache
			
			shopsStat.take(20).foreach(println)
			val average = shopsStat.map(_._2).mean
			println("=======>" + average)
	}

	def transactionsUniquePoints (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) {

		val ttPoints = trainTransactions.
						filter(t => t.transaction.transactionPoint.getX > 0).
						map(t => (t.transaction.customer_id, (t.transaction.transactionPoint, t.homePoint))).
						groupByKey.
						map{
							case (key, values) => 
								val v = values.groupBy(_._1).map(t => (t._1, values.head._2))
								(key, v)
						}.
						map(t => (1, (t._1, t._2))).
						cache

		ttPoints.
			join(ttPoints).
			map {
				case (key, (left, right)) =>
					val x  = left._2.head._2.getX  - right._2.head._2.getX
					val y  = left._2.head._2.getY  - right._2.head._2.getY 
					(left, right, math.sqrt(x * x + y * y)) 
			}.
			filter(t => t._3 <= TransactionClassifier.scoreRadious && t._1._1 != t._2._1).
			map {
				case (left, right, distance) => 
					val equalPointCount = left._2.map {
						case (lTransactionPoint, lHomePoint) =>
							var counter = 0
							right._2.foreach {
								case (rTransactionPoint, rHomePoint) =>
									val x  = lTransactionPoint.getX  - rTransactionPoint.getX
									val y  = lTransactionPoint.getY  - rTransactionPoint.getY 
									if (math.sqrt(x * x + y * y) <= 0.000001) {
										counter += 1
									}	
							}
							counter
					}.sum
					(left._1, right._1, distance, left._2.size, right._2.size, equalPointCount) 
			}.foreach(println)
	}

	def featurePointIdentify (conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext, transactions : RDD[Transaction],
								trainTransactions : RDD[TrainTransaction],
								column : String,
								equalPercent : Double 
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

									val clusters = DBSCAN2(TransactionClassifier.scoreRadious, 1).
													cluster(points.map(_._2))

									val maxPointCluster = clusters.
										map(t => (t.globalID, t)).
										groupBy(_._1).
										map{case (key, values) => (values, values.size)}.
										maxBy(_._2)

									val customers = maxPointCluster._1.map(t => pointIndexToCustomer.get(t._2.id).get)

									val percent = maxPointCluster._2.toDouble / points.size
									(point, percent, customers)
								}
						}.
						filter(t => t._2 >= equalPercent).
						filter(t => t._3.size > 0).
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
			filter(t => t._3 >= 0.5 && t._4 >= 3).
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
				equalPercent
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
						if (column == "workPoint") {
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

	def checkResult (conf : SparkConf, 
						sparkContext : SparkContext, 
						sqlContext : SQLContext, 
						transactions : RDD[Transaction],
						trainTransactions : RDD[TrainTransaction], 
						result : scala.collection.Map[String, Point]) {

		val definedCustomersCount = result.size
		
		val maxNearsPoints = transactions.
					map(t => (t.customer_id , t)).
					groupByKey.
					mapValues{
						case (values) =>
							val seq = values.toSeq
							values.map{
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
							}.maxBy(_._2)._1
					}.collectAsMap

		val maxNearsPointsCount = maxNearsPoints.size
		val scoredCount = maxNearsPoints.map{
				case (customer_id, point) => 
					val testPoint = result.get(customer_id).getOrElse(new Point(0,0))
					
					val x = testPoint.getX - point.getX
					val y = testPoint.getY - point.getY
					val distance = math.sqrt(x * x + y * y) 
					distance
			}.
			filter(t => t <= TransactionClassifier.scoreRadious).
			size

		val percent = scoredCount.toDouble / definedCustomersCount
		println(f"====> $definedCustomersCount $maxNearsPointsCount $percent")
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

	def trainDataAnalytics(conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext, transactions : RDD[TrainTransaction]) {
		
		val customerCount = transactions.map(_.transaction.customer_id).countByValue.size

		val customersInClusterCount = transactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			mapValues {
				case (groupedTrans) =>
					 val points = groupedTrans.
					 	zipWithIndex.
					 	map{
					 		case (t, index) => 
					 			DBSCANPoint(index + 1, t.transaction.transactionPoint.getX, t.transaction.transactionPoint.getY)
					 	} 

					val clusters = DBSCAN2(TransactionClassifier.scoreRadious, 3).
								cluster(points).
								map(p => (math.abs(p.clusterID), new Point(p.x, p.y))).
								filter(_._1 > 0).
								groupBy(_._1).
								map {
									case (clusterId, points) => 
										(clusterId, points, points.size)
								}

					var multiPoints = ListBuffer[MultiPoint]()
					
					if (clusters.size >= 2) {

						clusters.toSeq.sortWith(_._3 > _._3).slice(0, 2).foreach {
							case (clusterId, points, count) => 
								val mPoints = new MultiPoint()
								
								points.foreach {
									case (p) => 
										mPoints.add(p._2.getX, p._2.getY)
								}

								multiPoints += mPoints
						}

					} else {
						val mPoints = new MultiPoint()
						
						mPoints.add(
							groupedTrans.head.transaction.transactionPoint.getX, 
							groupedTrans.head.transaction.transactionPoint.getY
						)

						multiPoints += mPoints
					}
					var result = false
					multiPoints.foreach {
						case mPoints =>
							val convexHull = OperatorConvexHull.local().execute(mPoints, null)
							result = result || !OperatorDisjoint.local().execute(groupedTrans.head.homePoint, convexHull, null, null)		
					}
					result
			}.
			filter(!_._2).
			take(100).
			foreach(println(_))
	}

}