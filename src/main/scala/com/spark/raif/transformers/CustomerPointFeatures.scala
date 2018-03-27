/**
 *
 *
 * @author Lookuut Struchkov
 * @desc Transform customer transaction points to Customer character points 
 * 		 (points where was only current customer)
 * 	
 *
 */
 
package com.spark.raif.transformers


import org.apache.spark.rdd.RDD

import com.esri.core.geometry.Point
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint

import scala.reflect.runtime.{universe => ru}

import com.spark.raif.models._
import com.spark.raif.Consts

object CustomerPointFeatures {

	def perform(transactions : RDD[Transaction],
					trainTransactions : RDD[TrainTransaction],
					column : String,
					equalPercent : Double,
					equalPointPercent : Double,
					minEqualPointCount : Int) : Map[String, Point] = {
		
		val grouppedPoints = trainTransactions.
						map(t => (t.transaction.point, t)).
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

									val clusters = DBSCAN2(Consts.scoreRadious, minPointsCount).
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
						persist

		val customerToPointMap = grouppedPoints.
									map(t => t._2.map(tt => (tt._1, t._1))).
									flatMap(t => t).
									groupBy(_._1).
									collectAsMap
		
		val testPoints = transactions.
			filter(t => t.point.getX > 0).
			map(t => (t.point, t)).
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
					val trainCustomerPointCount = customerToPointMap.size
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
			perform(
				newTransactions.repartition(8), 
				trainTransactions.union(predictedTrainTransactions).repartition(8), 
				column,
				equalPercent,
				equalPointPercent,
				minEqualPointCount
			)
		} else {
			if (column == TrainTransaction.homePoint) {
				trainTransactions.
					map(t => (t.transaction.customer_id, t.homePoint)).
					collectAsMap.toMap
			} else {
				trainTransactions.
					map(t => (t.transaction.customer_id, t.workPoint)).
					collectAsMap.toMap
			}
		}
	}
}