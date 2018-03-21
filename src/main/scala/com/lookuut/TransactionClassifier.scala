package com.lookuut

import com.lookuut.utils.RichPoint
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
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import scala.util.Random
import org.apache.spark.mllib.clustering._

object TransactionClassifier {

	val scoreRadious = 0.02
	val squareScoreRadious = scoreRadious * scoreRadious
	
	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val paramsWidth = 5

	private val featuresCount = 9
	private val trainDataPart = 0.916

	def transactionTargetPoint(t : TrainTransaction, column : String) : Point = {
		if (column == "homePoint") 
			t.homePoint 
		else 
			t.workPoint
	}



	def pointSpectre (point: Point, transactions : Iterable[Transaction]) : org.apache.spark.mllib.linalg.Vector = {
		val transactionCount = transactions.size

		val mostVisitedPoint = transactions.
								filter(t => t.transactionPoint != point).
								map(t => (t.transactionPoint, 1)).
								groupBy(_._1).
								map(t => (t._1, t._2.size)).
								maxBy(_._2)._1

		val direction = RichPoint.vector(mostVisitedPoint, point)

		val pointWeightList = transactions.
							map(t => (t.transactionPoint, t)).
							groupBy(_._1).
							map {
								case (tPoint, pointTransactions) =>
									(
										tPoint, 
										pointTransactions.size, 
										TrainTransactionsAnalytics.squaredDistance(tPoint, point)
									)
							} 
		
		val pointWeight = pointWeightList.filter(t => t._1 == point).head._2

		val sortedSpectre = pointWeightList.
								toSeq.
								sortWith(_._3 < _._3).
								take(paramsWidth)

		val distanceSpectre = sortedSpectre.
						sliding(2).
						map(raw => 
							TrainTransactionsAnalytics.squaredDistance(raw(1)._1, raw(0)._1)	
						).
						toSeq

		val angleSpectre = 	sortedSpectre.map {
			case (p, frequent, distance) =>
				val v = RichPoint.vector(p, point)
				val cross = RichPoint.cross(v, direction)
				val dot = RichPoint.dot(v, direction) 
				Math.atan2(cross, dot)
		}.toSeq

		val buyFrequentSpectre = sortedSpectre.map(t => t._2.toDouble / transactionCount).toSeq

		Vectors.
			dense(
					(
						distanceSpectre ++
						angleSpectre ++ 
						buyFrequentSpectre					
					).
					toArray
				)
	}
	
	private val kMeasClusterCount = 150

	def train (ss : SparkSession,
				trainTransactions : RDD[(TrainTransaction)],
				targetPointType : String = "homePoint"
			) {

		val transactionsSpectre = trainTransactions.
			filter(t => TrainTransactionsAnalytics.
							squaredDistance(
								t.transaction.transactionPoint, 
								transactionTargetPoint(t, targetPointType)
							) < 1
			).
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter{
				case(customer_id, transactions) => 
					transactions.map(t => t.transaction.transactionPoint).toSet.size >= paramsWidth &&
					transactions.filter(t => TrainTransactionsAnalytics.
												squaredDistance(
													t.transaction.transactionPoint, 
													transactionTargetPoint(t, targetPointType)
												) <= squareScoreRadious
										).size > 0
			}.
			mapValues {
				case transactions => 
					val targetPoint = transactionTargetPoint(transactions.head, targetPointType)
					val tTransactions = transactions.map(t => t.transaction)

					transactions.
						filter(
							t => TrainTransactionsAnalytics.
									squaredDistance(t.transaction.transactionPoint, targetPoint) <= squareScoreRadious
						).
						map(t => t.transaction.transactionPoint).
						toSet.
						map((p : Point) => pointSpectre(
								p, 
								tTransactions
							)
						)
			}.
			map(t => t._2).
			flatMap(t => t).
			cache

		(5 to 160 by 5).
				map(k => (k, clusteringScore(ss, transactionsSpectre, k, f"kmeans-${targetPointType}-${k}"))).
				foreach(println)

		//val model = loadKMeansModel(sparkContext, targetPointType, kMeasClusterCount)
		/*
		val summary = Statistics.
						colStats(
							transactionsSpectre
						)

		println(summary.mean)
		println(summary.max)
		println(summary.min)
		println(summary.variance)
		*/
	} 

	def kmeansModelTest (ss : SparkSession,
							trainTransactions : RDD[(TrainTransaction)],
							targetPointType : String = "homePoint") {
		val random = new Random
		val model = loadKMeansModel(ss, targetPointType, kMeasClusterCount)

		val result = trainTransactions.
			filter(t => TrainTransactionsAnalytics.
							squaredDistance(
								t.transaction.transactionPoint, 
								transactionTargetPoint(t, targetPointType)
							) < 1
			).
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter{
				case(customer_id, transactions) => 
					val targetPoint = transactionTargetPoint(transactions.head, targetPointType)
					transactions.
						filter(t => 
								TrainTransactionsAnalytics.
									squaredDistance(t.transaction.transactionPoint, targetPoint) <= 
									squareScoreRadious
							)
						.size > 0 && transactions.
										map(t => t.transaction.transactionPoint).
										toSet.size >= paramsWidth &&
					transactions.
						filter(t => 
								TrainTransactionsAnalytics.
									squaredDistance(t.transaction.transactionPoint, targetPoint) > 
									squareScoreRadious
							).size > 0
			}.
			map {
				case (customer_id, transactions) => 
					val targetPoint = transactionTargetPoint(transactions.head, targetPointType)
					val customerTransactionCount = transactions.size
					val customerTransactions = transactions.map(t => t.transaction)

					val aroundTargetPointsSpectre = transactions.
						map(t => t.transaction.transactionPoint).
						filter(t => 
								TrainTransactionsAnalytics.
									squaredDistance(t, targetPoint) <= 
									squareScoreRadious
						).
						toSet.
						toList

					val aroundTargetPoint = aroundTargetPointsSpectre.
														apply(
															random.
																nextInt(
																	aroundTargetPointsSpectre.size
																)
															)

					val aroundTargetPointSpectre = pointSpectre(aroundTargetPoint, customerTransactions)
					
					
					val outPointMinDistance = transactions.
						filter(t => 
								TrainTransactionsAnalytics.
									squaredDistance(t.transaction.transactionPoint, targetPoint) >
									squareScoreRadious
						).
						map(t => t.transaction.transactionPoint).
						toSet.
						map((p: Point) =>  distToCentroid( 
								pointSpectre(p, customerTransactions),
								model
							)
						).
						minBy(t => t)

					val inPointDistance = distToCentroid(aroundTargetPointSpectre, model)
					
					Vectors.dense(
						inPointDistance, 
						outPointMinDistance, 
						if (inPointDistance < outPointMinDistance) 1 else 0
					)
			}

		val summary = Statistics.
						colStats(result)
		println(summary.mean)
		println(summary.max)
		println(summary.min)
	}

	def testSpectre (trainTransactions : RDD[TrainTransaction], targetPointType : String, count : Int = 1000) : RDD[org.apache.spark.mllib.linalg.Vector] = {
		val random = new Random

		trainTransactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter {
				case (customer_id, transactions) => 
					val targetPoint = transactionTargetPoint(transactions.head, targetPointType)
					transactions.
						map(t => (t.transaction.transactionPoint, t)).
						groupBy(_._1).
						filter{case (point, pointTransactions) => transactions.size >= paramsWidth}.
						map {
							case (point, pointTransactions) =>
								(
									point, 
									TrainTransactionsAnalytics.squaredDistance(point, targetPoint)
								)
						}.
						filter(t => t._2 > squareScoreRadious).size >= paramsWidth
			}.
			map {
				case (customer_id, transactions) => 
					val targetPoint = transactionTargetPoint(transactions.head, targetPointType)
					val customerTransactionCount = transactions.size

					val points = transactions.
						map(t => (t.transaction.transactionPoint, t)).
						groupBy(_._1).
						filter{case (point, pointTransactions) => transactions.size >= paramsWidth}.
						map {
							case (point, pointTransactions) =>
								(
									point, 
									TrainTransactionsAnalytics.squaredDistance(point, targetPoint)
								)
						}.
						filter(t => t._2 > squareScoreRadious).
						toList

					val pointCount = points.size
					val spectrePoint = points.apply(random.nextInt(pointCount))._1
					
					pointSpectre(
								spectrePoint, 
								transactions.map(t => t.transaction)
							)
			}
	}

	def entropy(counts: Iterable[Int]) = {
		
		val values = counts.filter(_ > 0)
		val n: Double = values.sum

		values.map { v =>
			val p = v / n
			-p * math.log(p)
		}.sum
	}

	def clusteringScore(ss : SparkSession, data : RDD[Vector], k : Int, modelName : String = "") = {

		val kmeans = new KMeans()

		kmeans.setK(k)
		kmeans.setRuns(20)
		kmeans.setEpsilon(1.0e-6)

		val model = kmeans.run(data)

		if (modelName != "") {
			model.save(ss.sparkContext, f"${trainedModelsPath}${modelName}")
		}
		
		data.map(datum => distToCentroid(datum, model)).mean()
	}

	def standartScore(data : RDD[Vector]) {
		val dataAsArray = data.map(_.toArray)
		val numCols = dataAsArray.first().length
		val n = dataAsArray.count()
		val sums = dataAsArray.reduce(
						(a,b) => a.zip(b).map(t => t._1 + t._2)
					)
		
		val sumSquares = dataAsArray.aggregate(
				new Array[Double](numCols)
			)(
				(a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
				(a, b) => a.zip(b).map(t => t._1 + t._2)
			)
		val stdevs = sumSquares.zip(sums).map {
			case(sumSq,sum) => math.sqrt(n*sumSq - sum*sum)/n
		}

		val means = sums.map(_ / n)
		
		def normalize(datum: Vector) = {
			val normalizedArray = (datum.toArray, means, stdevs).
								zipped.
								map(
									(value, mean, stdev) =>
										if (stdev <= 0) 
											(value - mean) 
										else 
											(value - mean) / stdev
								)
			Vectors.dense(normalizedArray)
		}
	}
	
	def distance(a: Vector, b: Vector) =
		math.sqrt(a.toArray.zip(b.toArray).
		map(p => p._1 - p._2).map(d => d * d).sum)
	
	def distToCentroid(datum: Vector, model: KMeansModel) = {
		val cluster = model.predict(datum)
		val centroid = model.clusterCenters(cluster)
		distance(centroid, datum)
	}

	def loadKMeansModel (ss : SparkSession, targetPointType : String, kMeasClusterCount : Int) : KMeansModel = {
		KMeansModel.load(ss.sparkContext, f"${trainedModelsPath}kmeans-${targetPointType}-${kMeasClusterCount}")
	}

	def loadDecisionTree (ss : SparkSession, targetPointType : String, modelName : String) : DecisionTreeModel = {
		DecisionTreeModel.load(ss.sparkContext, f"$trainedModelsPath$targetPointType-$modelName")
	}

	def prediction(
			ss : SparkSession, 
			toPredictTransactions : RDD[Transaction], 
			trainTransactions : RDD[(Point, TrainTransaction)],
			targetPointType : String = "homePoint",
			modelName : String
		) : Map[String, Point] = {

		Map("" -> new Point(0.0, 0.0))
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

	var mccCategories = scala.collection.Map[Int, Long]()

	def getMccCategories (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : scala.collection.Map[Int, Long] = {
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
