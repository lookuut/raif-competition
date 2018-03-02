package com.lookuut

import java.util.Calendar
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.esri.core.geometry._

import org.apache.spark.util.StatCounter

import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint

import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import scala.reflect.runtime.{universe => ru}

object TransactionClassifier {

	val scoreRadious = 0.02
	val squareScoreRadious = 0.0004
	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val paramsWidth = 5

	private val featuresCount = 2
	private val trainDataPart = 0.8


	def targetPointToIndex (trainTransactions : RDD[(Point, TrainTransaction)], targetPointType : String = "homePoint") : Map[Point, Int] = {

		trainTransactions.
			map(t => (t._1.getX.toString + t._1.getY.toString, t._1)).
			sortBy(_._1).
			groupByKey.
			sortByKey().
			mapValues{case(values) => values.head}.
			zipWithUniqueId.
			map{
				case((key, t), index) => 
				(t, index.toInt)
			}.
			collect.toMap
	}

	def transactionToFeatures (transaction : Transaction) : List[Double] = {
		List(transaction.transactionPoint.getX, transaction.transactionPoint.getY)
	}

	def train (conf : SparkConf, 
				sparkContext : SparkContext, 
				sqlContext : SQLContext, 
				trainTransactions : RDD[(TrainTransaction)],
				targetPointType : String = "homePoint"
			) {

		val indexedByPointTrans = trainTransactions.
			map{
				case (t) =>
					val keyPoint = if (targetPointType == BankTransactions.homePointType) t.homePoint else t.workPoint
					(keyPoint, t)
			}.cache

		val clusteredPoints = targetPointToIndex(indexedByPointTrans, targetPointType)

		val data = indexedByPointTrans.
			groupByKey.
			mapValues{
				case groupedTrans =>
					var data = ListBuffer[Seq[Double]]()
					var buffer = ListBuffer[Double]()
					var index = 0
					val customerTransactions = groupedTrans.
						map{case (t) =>
							val tPoint = t.transaction.transactionPoint
							
							val mirror = ru.runtimeMirror(t.getClass.getClassLoader)
							val shippedTrainedPoint = ru.typeOf[TrainTransaction].decl(ru.TermName(targetPointType)).asTerm
							val im = mirror.reflect(t)
							val shippingPointFieldMirror = im.reflectField(shippedTrainedPoint)

							val hPoint = shippingPointFieldMirror.get.asInstanceOf[Point]

							val x = (tPoint.getX - hPoint.getX) 
							val y = (tPoint.getY - hPoint.getY)
							val squaredDistance = x * x + y * y
							(squaredDistance, t)
						}

					val nearPurposePointTransactions = customerTransactions.
						filter(t => t._1 <= squareScoreRadious)
					
					if (nearPurposePointTransactions.size > 0) {
						scala.util.Random.shuffle(nearPurposePointTransactions).
							foreach {
								case (d, t) =>
									buffer ++= transactionToFeatures(t.transaction)
									index += 1

									if (index >= paramsWidth) {
										data += buffer.toSeq
										buffer = ListBuffer[Double]()
										index = 0
									}
							}
					} else {
						val t = customerTransactions.toSeq.sortWith(_._1 > _._1).head._2
						buffer ++= transactionToFeatures(t.transaction)
					}

					if (buffer.size > 0) {
						val lastPos = buffer.size
						for (i <- buffer.size / featuresCount to paramsWidth if i < paramsWidth) {
							for (j <- 0 to featuresCount - 1) {
								buffer += buffer(lastPos - (featuresCount - j))
							}
						}	
						data += buffer.toSeq
					}
					
					data.toSeq
			}.
			flatMap{case (point, values) => values.map((point, _))}.
			map{
				case (point, params) => 
					LabeledPoint(
						clusteredPoints.get(point).get.toDouble,
						Vectors.dense(
							params.toArray
						)
					)
			}

		val Array(_trainData, _cvData) = data.randomSplit(Array(trainDataPart, 1 - trainDataPart))
		val trainData = _trainData.cache() 
		val cvData = _cvData.cache()
		// Train a DecisionTree model.
		//  Empty categoricalFeaturesInfo indicates all features are continuous.
		val categoricalFeaturesInfo = Map[Int, Int]()

		val evaluations = for (
								impurity <- Array("entropy");
								depth <- Array(20);
								bins <- Array(300)
							) yield {
								
								val model = DecisionTree.trainClassifier(
									trainData, clusteredPoints.valuesIterator.max.toInt + 1, categoricalFeaturesInfo,
									impurity, depth, bins)

								val trainAccuracy = getMetrics(model, trainData).precision
								val cvAccuracy = getMetrics(model, cvData).precision
								model.save(sparkContext, trainedModelsPath + f"$targetPointType-$impurity-$depth-$bins-msk-" + Calendar.getInstance().getTimeInMillis().toString)
								println(f"======>$impurity, $depth, $bins, $trainAccuracy, $cvAccuracy")
								((impurity, depth, bins), (trainAccuracy, cvAccuracy))
							}

		evaluations.sortBy(_._2).reverse.foreach(println)
	} 

	def loadDecisionTree (sc : SparkContext, targetPointType : String) : DecisionTreeModel = {
		DecisionTreeModel.load(sc, trainedModelsPath + targetPointType + "-entropy-20-300")
	}

	def prediction(
			conf : SparkConf, 
			sparkContext : SparkContext, 
			sqlContext : SQLContext, 
			toPredictTransactions : RDD[Transaction], 
			trainTransactions : RDD[(Point, TrainTransaction)],
			targetPointType : String = "homePoint"
		) : Map[String, Point] = {

		val model = loadDecisionTree(sparkContext, targetPointType)
		val zippedTransactionsPoints = prepareTestData(conf, sparkContext, sqlContext, toPredictTransactions).
										collect

		val indexToPoint = targetPointToIndex(trainTransactions, targetPointType).map(t => (t._2, t._1)).toMap

		zippedTransactionsPoints.
			map{
				case(customer_id, features) => 
					val predictedPointIndex = model.predict(Vectors.dense(features.toArray))
					val point = indexToPoint.get(predictedPointIndex.toInt)
					(customer_id, point)
			}.
			groupBy(_._1).
			map{
				case (customer_id, groupedPoints) =>
					groupedPoints.
						map(p => (p._2.get.getX.toString + p._2.get.getY.toString, p._2.get)).
						groupBy(_._1).
						map {
							case (pointStr, p) => 
								(customer_id, p.head._2, p.size)
						}.maxBy(_._3)
			}.
			map(t => (t._1, t._2)).
			toMap
	}
	
	def prepareTestData (conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext, transactions : RDD[Transaction]) : RDD[(String,Seq[Double])] = {
		transactions.
			map{case (t) => 
				var features = ListBuffer[Double]()
				for (i <- 0 to paramsWidth) {
					features ++= transactionToFeatures(t)
				}
				(t.customer_id, features.toSeq)
			}.cache
		/*
		transactions.
			map(t => (t.customer_id, t)).
			groupByKey.
			mapValues {
				case customerTransactions => 
					val pointsToTrans = customerTransactions.
						zipWithIndex.
						map{case(t, index) =>
							(index, t)
						}.toMap

					val points = pointsToTrans.
						map{
							case(index, t) => 
								DBSCANPoint(index, t.transactionPoint.getX, t.transactionPoint.getY)
						}

					val clusteredPoints = DBSCAN2(scoreRadious / 2, 2).
											cluster(points).
											map(p => (math.abs(p.clusterID), p))
					val maxClusterID = clusteredPoints.maxBy(_._1)._1 + 1

					val zeroClusterPoints = clusteredPoints.
						filter(_._1 == 0).
						zipWithIndex.
						map{
							case((clusterId, point), index) =>
								(index + maxClusterID, point)
						}

					val testData = (zeroClusterPoints ++ clusteredPoints.filter(_._1 > 0)).
						map(t => (t._1, t._2)).
						groupBy(_._1).
						mapValues{
							case points => 
								(points.size, points)
						}.
						map {
							case (clusterId, (pointCount, clusterPoints)) => 
								val transactions = clusterPoints.
									map(p => (pointsToTrans(p._2.id.toInt)))
								
								pointsToVector(transactions)
						}.
						flatten
					testData
			}.
			flatMap{case (customer_id, transactions) => transactions.map((customer_id, _))}.
			cache*/
	}
	
	def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) : MulticlassMetrics = {
		val predictionsAndLabels = data.map(example =>
			(model.predict(example.features), example.label)
		)
		new MulticlassMetrics(predictionsAndLabels)
	}


	def pointsToVector(transactions : Iterable[Transaction]) : Seq[Seq[Double]] = {
		var data = ListBuffer[Seq[Double]]()
		var buffer = ListBuffer[Double]()
		var index = 0

		scala.util.Random.shuffle(transactions).
			foreach {
				case (t) =>
					val point = t.transactionPoint
					buffer += point.getX
					buffer += point.getY
					buffer += t.amount.get
					buffer += (if (t.atmPoint.getX > 0) 1 else 0).toDouble
					index += 1

					if (index >= paramsWidth) {
						data += buffer.toSeq
						buffer = ListBuffer[Double]()
						index = 0
					}
			}
	
		if (buffer.size > 0) {
			val lastPos = buffer.size
			for (i <- buffer.size / 4 to paramsWidth if i < paramsWidth) {
				buffer += buffer(lastPos - 4)
				buffer += buffer(lastPos - 3)
				buffer += buffer(lastPos - 2)
				buffer += buffer(lastPos - 1)
			}	
			data += buffer.toSeq
		}
		
		data.toSeq
	}

}