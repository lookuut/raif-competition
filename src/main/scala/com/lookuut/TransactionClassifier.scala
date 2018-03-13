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
import org.joda.time.DateTime


object TransactionClassifier {

	val scoreRadious = 0.02
	val squareScoreRadious = scoreRadious * scoreRadious
	
	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val paramsWidth = 1

	private val featuresCount = 9
	private val trainDataPart = 1.0


	def targetPointToIndex (
								trainTransactions : RDD[(Point, TrainTransaction)], 
								targetPointType : String = "homePoint"
	) : Map[Point, Int] = {

		val clusters = DBSCAN2(scoreRadious, 4).
			cluster(
				trainTransactions.
					map(t => (t._1.getX.toString + t._1.getY.toString, t._1)).
					sortBy(_._1).
					groupByKey.
					sortByKey().
					mapValues{case(values) => values.head}.
					zipWithUniqueId.
					map{
						case((key, t), index) => 
						DBSCANPoint(index.toInt, t.getX, t.getY)
					}.
					collect.
					toSeq
			).map(p => (math.abs(p.clusterID), p)) 
		
		val clusterStartID = clusters.filter(p => p._1 == 0).size + 1
		val noises = clusters.filter(p => p._1 == 0).
						zipWithIndex.map{case ((clusterID, p), index) => (new Point(p.x, p.y), index)}.
						toMap

		val clusteredPoints = clusters.filter(p => p._1 > 0).
									map(p => (new Point(p._2.x, p._2.y), p._2.clusterID + clusterStartID)).
									toMap

		val points = (noises ++ clusteredPoints)
		println("clusters count " + points.map{case (k,v) => v}.toSet.size + " points count " + points.size)
		points
	}

	def transactionToFeatures (t : Transaction) : List[Double] = {

		val point = t.transactionPoint
		val cityClusterID = TransactionPointCluster.getPointCluster(point).toDouble
		val countryID = TransactionClassifier.countriesCategories.get(t.country.getOrElse("")).get.toDouble
		val dayOfWeek = TransactionClassifier.getDateCategory(t.transactionDate.get).toDouble
		val operationType = t.operationType.toDouble
		val amount = t.amountPower10
		val currencyID = TransactionClassifier.currencyCategories.get(t.currency).get.toDouble
		val mccID = TransactionClassifier.mccCategories.get(t.mcc).get.toDouble

		List(t.transactionPoint.getX, 
				t.transactionPoint.getY, 
				countryID, 
				cityClusterID,
				dayOfWeek,
				operationType,
				amount,
				currencyID,
				mccID
			)
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
			}.
			persist

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

					val nearPurposePointTransactions = customerTransactions
						.filter(t => t._1 <= 5 * squareScoreRadious)
					
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

		indexedByPointTrans.unpersist(true)

		val categoricalFeaturesInfo = (
			for (ii <- 0 to paramsWidth - 1) yield {
				Map(
					(ii * featuresCount + 2) -> countriesCategories.size, 
					(ii * featuresCount + 3) -> TransactionPointCluster.getClustersCount, 
					(ii * featuresCount + 4) -> dateCategories.size,
					(ii * featuresCount + 5) -> 2,
					(ii * featuresCount + 7) -> TransactionClassifier.currencyCategories.size,
					(ii * featuresCount + 8) -> TransactionClassifier.mccCategories.size
				)
			}
		).flatMap(t => t).toMap 

		val evaluations = for (
								impurity <- Array("gini");
								depth <- Array(20);
								bins <- Array(800)
							) yield {
								val model = DecisionTree.trainClassifier(
									trainData, clusteredPoints.valuesIterator.max.toInt + 1, categoricalFeaturesInfo,
									impurity, depth, bins)

								//val trainAccuracy = getMetrics(model, trainData).accuracy
								//val cvAccuracy = getMetrics(model, cvData).accuracy
								model.save(sparkContext, trainedModelsPath + f"$targetPointType-$impurity-$depth-$bins-with-params")
								//((impurity, depth, bins), (trainAccuracy, cvAccuracy))
							}

		//evaluations.sortBy(_._2._2).reverse.foreach(println)
	} 

	def loadDecisionTree (sc : SparkContext, targetPointType : String, modelName : String) : DecisionTreeModel = {
		DecisionTreeModel.load(sc, f"$trainedModelsPath$targetPointType-$modelName")
	}

	def prediction(
			conf : SparkConf, 
			sparkContext : SparkContext, 
			sqlContext : SQLContext, 
			toPredictTransactions : RDD[Transaction], 
			trainTransactions : RDD[(Point, TrainTransaction)],
			targetPointType : String = "homePoint",
			modelName : String
		) : Map[String, Point] = {


		val customersPoints = toPredictTransactions.
			map(t => (t.customer_id, t.transactionPoint)).
			groupByKey.
			collectAsMap

		val model = loadDecisionTree(sparkContext, targetPointType, modelName)
		val zippedTransactionsPoints = prepareTestData(conf, sparkContext, sqlContext, toPredictTransactions).
										collect

		val clusterIDToPoint = targetPointToIndex(trainTransactions, targetPointType).
								groupBy(_._2).toMap

		zippedTransactionsPoints.
			map{
				case(customer_id, features) => 
					val clusterID = model.predict(Vectors.dense(features.toArray)).toInt
					(customer_id, clusterID)
			}.
			groupBy(_._1).
			map{
				case (customer_id, groupedClusters) =>
					val clusterID = groupedClusters.
						map(p => (p._2, 1)).
						groupBy(_._1).
						map {
							case (p, rows) => 
								(p, rows.size)
						}.
						maxBy(_._2)._1

					val cluster = clusterIDToPoint.get(clusterID).get
					val points = customersPoints.get(customer_id).get


					val inClusterPoints = points.filter {
						case p => 
							val inClusterPointCount = cluster.
								filter(clusterPoint => TrainTransactionsAnalytics.distance(clusterPoint._1, p) <= scoreRadious).
								size
							inClusterPointCount > 0	
					} 
					
					val maxNearPoints = inClusterPoints.map {
							case point => 
								val nearCount = inClusterPoints.filter(p => p != point).filter {
									case p => 
										TrainTransactionsAnalytics.distance(point, p) <= scoreRadious
								}.size
								(point , nearCount)
						}

					(
						customer_id , 
						if (maxNearPoints.size > 0) maxNearPoints.maxBy(_._2)._1 else new Point(0,0)
					)
			}.
			map(t => (t._1, t._2)).
			toMap
	}
	
	def prepareTestData (conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext, transactions : RDD[Transaction]) : RDD[(String,Seq[Double])] = {

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
					val minPointCount = if (points.size >= 4) 4 else points.size

					val clusteredPoints = DBSCAN2(scoreRadious, minPointCount).
											cluster(points).
											map(p => (math.abs(p.clusterID), p))

					val cPoints =  						
					{
						if (clusteredPoints.filter(p => p._1 > 0).size > 0) 
							clusteredPoints.filter(p => p._1 > 0)
						else 
							clusteredPoints
					}						

					cPoints.
						groupBy(_._1).
						map{
							case (clusterID, clusterPoints) =>
								var data = ListBuffer[Seq[Double]]()
								var buffer = ListBuffer[Double]()
								var index = 0
								
								val clusterTransactions = clusterPoints.map(t => pointsToTrans(t._2.id.toInt))
								if (clusterPoints.size > 0) {
									clusterTransactions.
										foreach {
											case (t) =>
												buffer ++= transactionToFeatures(t)
												index += 1

												if (index >= paramsWidth) {
													data += buffer.toSeq
													buffer = ListBuffer[Double]()
													index = 0
												}
										}
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
						flatten
			}.
			flatMap{case (customer_id, transactions) => transactions.map((customer_id, _))}.
			cache
	}
	
	def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) : MulticlassMetrics = {
		val predictionsAndLabels = data.map(example =>
			(model.predict(example.features), example.label)
		)
		new MulticlassMetrics(predictionsAndLabels)
	}


	val dateCategories = scala.collection.Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6)

	def getDateCategory (date : DateTime) : Int = {
		date.getDayOfWeek - 1
	}

	var countriesCategories = scala.collection.Map[String, Long]()

	def getCountriesCategories (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : scala.collection.Map[String, Long] = {
		if (countriesCategories.size == 0) {
			countriesCategories = trainTransactions.
				map(t => t.transaction.country.getOrElse("")).
				union(
					transactions.map(t => t.country.getOrElse(""))
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (country, index) => (country, index)}.
				collectAsMap
		}
		
		countriesCategories
	}

	var currencyCategories = scala.collection.Map[Int, Long]()

	def getCurrencyCategories (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : scala.collection.Map[Int, Long] = {
		if (currencyCategories.size == 0) {
			currencyCategories = trainTransactions.
				map(t => t.transaction.currency).
				union(
					transactions.map(t => t.currency)
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (currency, index) => (currency, index)}.
				collectAsMap
		}
		
		currencyCategories
	}

	var mccCategories = scala.collection.Map[String, Long]()

	def getMccCategories (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : scala.collection.Map[String, Long] = {
		if (mccCategories.size == 0) {
			mccCategories = trainTransactions.
				map(t => t.transaction.mcc).
				union(
					transactions.map(t => t.mcc)
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (currency, index) => (currency, index)}.
				collectAsMap
		}
		
		mccCategories
	}
}
