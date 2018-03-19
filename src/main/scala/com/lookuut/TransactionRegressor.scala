package com.lookuut

import java.util.Calendar
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.esri.core.geometry.Point

import org.apache.spark.util.StatCounter

import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import scala.reflect.runtime.{universe => ru}
import org.joda.time.DateTime


object TransactionRegressor {
	def transactionVector(t : Transaction, ratio : Double) : Array[Double] = {
		val point = t.transactionPoint
		val cityClusterID = TransactionPointCluster.getPointCluster(t.transactionPoint).toDouble
		val countryID = TransactionClassifier.countriesCategories.get(t.country.getOrElse("")).get.toDouble
		val dayOfWeek = TransactionClassifier.getDateCategory(t.date.get).toDouble
		val operationType = t.operationType.toDouble
		val amount = t.amountPower10
		val currencyID = TransactionClassifier.currencyCategories.get(t.currency).get.toDouble
		val mcc = TransactionClassifier.mccCategories.get(t.mcc).get.toDouble

		Array(
			amount,
			currencyID, 
			cityClusterID, 
			countryID, 
			mcc,
			dayOfWeek, 
			operationType, 
			ratio
		)
	}
}

class TransactionRegressor(private val sparkContext : SparkContext) {

	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val trainDataPart = 1.0


	def prepareTrainData (trainTransactions : RDD[(TrainTransaction)],
				column : String = "homePoint"
			) : RDD[LabeledPoint] = {

		trainTransactions.
			filter(t => (if (column == "homePoint") t.homePoint else t.workPoint).getX > 0).
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			map{
				case (customer_id, customerTransactions) => 
					customerTransactions.
					map(t => (t.transaction.transactionPoint ,t )).
					groupBy(_._1).
					map{
						case (transactionPoint, pointTransactions) =>
							pointTransactions.map {
								case tt => 
									val t = tt._2
									val point = t.transaction.transactionPoint
									val isHome = TrainTransactionsAnalytics.distance(point, t.homePoint) <= TransactionClassifier.scoreRadious
									val isWork = TrainTransactionsAnalytics.distance(point, t.workPoint) <= TransactionClassifier.scoreRadious
									val ratio = pointTransactions.size.toDouble / customerTransactions.size
									
									(TransactionRegressor.transactionVector(t.transaction, ratio), isHome, isWork)
							}
					}.flatMap(t => t)					
			}.
			flatMap(t => t).
			map{
				case t => 
					val purpose = if (column == "homePoint") t._2 else t._3
					LabeledPoint(
						if (purpose) 1.0 else 0.0,
						Vectors.dense(
							t._1
						)
					)
			}
	}

	def categoricalFeaturesInfo () : Map[Int, Int] = {
		Map(
			1 -> TransactionClassifier.currencyCategories.size,
			2 -> TransactionPointCluster.getClustersCount, 
			3 -> TransactionClassifier.countriesCategories.size, 
			4 -> TransactionClassifier.mccCategories.size,
			5 -> TransactionClassifier.dateCategories.size,
			6 -> 2
		)
	}

	def train (trainTransactions : RDD[(TrainTransaction)],
				column : String = "homePoint"
			) : RandomForestModel = {
		val data = prepareTrainData(trainTransactions, column)

		val model = RandomForest.trainClassifier(
									data, 2, categoricalFeaturesInfo,
									20, "auto",
									"gini", 5, 1000)
		
		model.save(sparkContext, trainedModelsPath + f"$column-gini-10-1000-with-params-binary")
		return model
	} 

	def generateModel(trainTransactions : RDD[TrainTransaction], 
						transactions : RDD[Transaction],
						column : String = "homePoint",
						trainDataPart : Double = 1.0) {
		
		val data = prepareTrainData(trainTransactions, column)
		val testData = prepareTestData(transactions)
		
		data.take(100).foreach(println)
		println("Train transaction count " + data.count)
		println("Test transaction count " + testData.count)
		testData.take(100).foreach(println)
		/*
		val Array(_trainData, _cvData) = data.randomSplit(Array(trainDataPart, 1 - trainDataPart))
		
		val trainData = _trainData.cache() 
		val cvData = _cvData.cache()

		val evaluations = for (
								impurity <- Array("gini", "entropy");
								depth <- Array(5, 7, 10);
								bins <- Array(800, 1000)
							) yield {
								val model = DecisionTree.trainClassifier(
									trainData, 2, categoricalFeaturesInfo,
									impurity, depth, bins)

								val trainAccuracy = getMetrics(model, trainData).accuracy
								val cvAccuracy = getMetrics(model, cvData).accuracy
								val predictedCustomerCount = prediction(testData, column, model).size
								((impurity, depth, bins), (trainAccuracy, cvAccuracy, predictedCustomerCount))
							}

		evaluations.sortBy(_._2._2).reverse.foreach(println)*/
	}	

	def loadDecisionTree (sc : SparkContext, targetPointType : String, modelName : String) : DecisionTreeModel = {
		DecisionTreeModel.load(sc, f"$trainedModelsPath$targetPointType-$modelName")
	}


	def prepareTestData (transactions : RDD[Transaction]
			) : RDD[(String, (Point, org.apache.spark.mllib.linalg.Vector))] = {

		transactions.
			map(t => (t.customer_id, t)).
			groupByKey.
			map{
				case (customer_id, customerTransactions) => 
					val trans = customerTransactions.
						map(t => (t.transactionPoint , t)).
						groupBy(_._1).
						map{
							case (transactionPoint, pointTransactions) =>
								pointTransactions.map {
									case tt => 
										val t = tt._2
										val ratio = pointTransactions.size.toDouble / customerTransactions.size
										
										(t.transactionPoint, Vectors.dense(TransactionRegressor.transactionVector(t, ratio)))
								}
						}.
						flatMap(t => t)

					(customer_id, trans)
			}.
			map(t => t._2.map(tt => (t._1, tt))).
			flatMap(t => t)
	}

	def prediction(
			toPredictTransactions : RDD[(String, (Point, org.apache.spark.mllib.linalg.Vector))], 
			targetPointType : String = "homePoint",
			model : DecisionTreeModel
		) : scala.collection.Map[String, Point] = {
		
		val prediction = toPredictTransactions.
			map(t=> (t._1, t._2._1, model.predict(t._2._2))).
			filter(t => t._3 == 1).
			map(t => (t._1, t._2)).
			groupByKey.
			mapValues{
				case points =>
					points.map {
						case (pLeft) => 
							val nearsCount = points.map {
											case(pRight) => TrainTransactionsAnalytics.distance(pLeft, pRight)
										}.filter(t => t <= TransactionClassifier.scoreRadious).
										size
						(pLeft, nearsCount)
					}.
					maxBy(_._2)._1
			}.
			cache

		val customersCount = prediction.map(t => t._1).distinct.count
		val customersWithFewPointsCount = prediction.
			map(t => (t._1, t._2)).
			groupByKey.
			map{
				case (key, t) => 
					(key, t, t.size)
			}.
			filter(t => t._3 > 1).count
		println(f"========++> $customersCount and $customersWithFewPointsCount")

		prediction.
			map(t => (t._1, t._2)).
			groupByKey.
			mapValues{
				case t => 
					t.head
			}.
			collectAsMap
	}
	
	
	def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) : MulticlassMetrics = {
		val predictionsAndLabels = data.map(example =>
			(model.predict(example.features), example.label)
		)
		new MulticlassMetrics(predictionsAndLabels)
	}
}
