package com.spark.raif.solutions

import org.apache.spark.sql.SparkSession

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint
import com.esri.core.geometry.Point

import scala.util.Random

import com.spark.raif.models._
import com.spark.raif.Consts
import com.spark.raif.SparkApp

import com.spark.utils.geometry.PointUtils._

object KMeansClustering {

	private val paramsWidth = 5

	private val featuresCount = 9
	private val trainDataPart = 0.9


	def pointSpectre (point: Point, transactions : Iterable[Transaction]) : Vector = {
		val transactionCount = transactions.size

		val mostVisitedPoint = transactions.
								filter(t => t.point != point).
								map(t => (t.point, 1)).
								groupBy(_._1).
								map(t => (t._1, t._2.size)).
								maxBy(_._2)._1

		val pointWeightList = transactions.
							map(t => (t.point, t)).
							groupBy(_._1).
							map {
								case (tPoint, pointTransactions) =>
									(
										tPoint, 
										pointTransactions.size, 
										point.squareDistance(tPoint)
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
									raw(1)._1.squareDistance(raw(0)._1)
								).
								toSeq

		val buyFrequentSpectre = sortedSpectre.map(t => t._2.toDouble / transactionCount).toSeq

		Vectors.
			dense(
					(
						distanceSpectre ++
						buyFrequentSpectre					
					).
					toArray
				)
	}
	
	private val kMeasClusterCount = 150

	def train (ss : SparkSession,
				trainTransactions : RDD[(TrainTransaction)],
				column : String = TrainTransaction.homePoint
			) {

		val transactionsSpectre = trainTransactions.
			filter(t => 	
						t.transaction.
							point.
							squareDistance(t.getTargetPoint(column)) < 1
			).
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter{
				case(customer_id, transactions) => 
					transactions.map(t => t.transaction.point).toSet.size >= paramsWidth &&
					transactions.filter(t => t.transaction.point.inRadios(t.getTargetPoint(column), Consts.scoreRadious)
										).size > 0
			}.
			mapValues {
				case transactions => 
					val targetPoint = transactions.head.getTargetPoint(column)
					val tTransactions = transactions.map(t => t.transaction)

					transactions.
						filter(
							t => t.transaction.point.inRadios(targetPoint, Consts.scoreRadious)
						).
						map(t => t.transaction.point).
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
				map(k => (k, clusteringScore(ss, transactionsSpectre, k, f"kmeans-${column}-${k}"))).
				foreach(println)
	} 

	def kmeansModelTest (ss : SparkSession,
							trainTransactions : RDD[(TrainTransaction)],
							column : String = TrainTransaction.homePoint) 
	: MultivariateStatisticalSummary = 
	{
		val random = new Random
		val model = loadKMeansModel(ss, column, kMeasClusterCount)

		val result = trainTransactions.
			filter(t => t.transaction.point.inRadios(t.getTargetPoint(column), 1)).
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter{
				case(customer_id, transactions) => 
					val targetPoint = transactions.head.getTargetPoint(column)
					transactions.
						filter(t => t.transaction.point.inRadios(targetPoint, Consts.scoreRadious))
						.size > 0 && transactions.
										map(t => t.transaction.point).
										toSet.size >= paramsWidth &&
					transactions.
						filter(t => 
								t.transaction.point.squareDistance(targetPoint) > Consts.squareScoreRadious
							).size > 0
			}.
			map {
				case (customer_id, transactions) => 
					val targetPoint = transactions.head.getTargetPoint(column)
					val customerTransactionCount = transactions.size
					val customerTransactions = transactions.map(t => t.transaction)

					val aroundTargetPointsSpectre = transactions.
						map(t => t.transaction.point).
						filter(t => 
								t.inRadios(targetPoint, Consts.scoreRadious)
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
						filter(t => t.transaction.point.inRadios(targetPoint, Consts.scoreRadious)).
						map(t => t.transaction.point).
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

		Statistics.colStats(result)
	}

	def testSpectre (trainTransactions : RDD[TrainTransaction], 
						column : String = TrainTransaction.homePoint, 
						count : Int = 1000) : RDD[org.apache.spark.mllib.linalg.Vector] = {
		val random = new Random

		trainTransactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			filter {
				case (customer_id, transactions) => 
					val targetPoint = transactions.head.getTargetPoint(column)
					transactions.
						map(t => (t.transaction.point, t)).
						groupBy(_._1).
						filter{case (point, pointTransactions) => transactions.size >= paramsWidth}.
						map {
							case (point, pointTransactions) =>
								(
									point, 
									point.squareDistance(targetPoint)
								)
						}.
						filter(t => t._2 > Consts.squareScoreRadious).size >= paramsWidth
			}.
			map {
				case (customer_id, transactions) => 
					val targetPoint = transactions.head.getTargetPoint(column)
					val customerTransactionCount = transactions.size

					val points = transactions.
						map(t => (t.transaction.point, t)).
						groupBy(_._1).
						filter{case (point, pointTransactions) => transactions.size >= paramsWidth}.
						map {
							case (point, pointTransactions) =>
								(
									point, 
									point.squareDistance(targetPoint)
								)
						}.
						filter(t => t._2 > Consts.squareScoreRadious).
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

	def loadKMeansModel (ss : SparkSession, targetPoint : String, kMeasClusterCount : Int) : KMeansModel = {
		KMeansModel.load(ss.sparkContext, f"${SparkApp.modelsDir}kmeans-${targetPoint}-${kMeasClusterCount}")
	}
}