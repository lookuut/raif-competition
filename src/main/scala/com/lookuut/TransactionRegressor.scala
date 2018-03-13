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

	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val trainDataPart = 0.8
	
	def train (conf : SparkConf, 
				sparkContext : SparkContext, 
				sqlContext : SQLContext, 
				trainTransactions : RDD[(TrainTransaction)],
				column : String = "homePoint"
			) {

		val data = trainTransactions.map{
				case t => 
					val point = t.transaction.transactionPoint
					val cityClusterID = TransactionPointCluster.getPointCluster(point).toDouble
					val countryID = TransactionClassifier.countriesCategories.get(t.transaction.country.getOrElse("")).get.toDouble
					val dayOfWeek = TransactionClassifier.getDateCategory(t.transaction.transactionDate.get).toDouble
					val operationType = t.transaction.operationType.toDouble
					val isHome = TrainTransactionsAnalytics.distance(point, t.homePoint) <= TransactionClassifier.scoreRadious
					val isWork = TrainTransactionsAnalytics.distance(point, t.workPoint) <= TransactionClassifier.scoreRadious
					val amount = t.transaction.amountPower10
					val currencyID = TransactionClassifier.currencyCategories.get(t.transaction.currency).get.toDouble
					(Array(cityClusterID, countryID, dayOfWeek, operationType, amount, currencyID, point.getX, point.getY), isHome, isWork)
			}.map{
				case t => 
					val purpose = if (column == "homePoint") t._2 else t._3
					LabeledPoint(
						if (purpose) 1.0 else 0.0,
						Vectors.dense(
							t._1
						)
					)
			}
									

		val categoricalFeaturesInfo = Map(
					0 -> TransactionPointCluster.getClustersCount, 
					1 -> TransactionClassifier.countriesCategories.size, 
					2 -> TransactionClassifier.dateCategories.size,
					3 -> 3,
					5 -> TransactionClassifier.currencyCategories.size
				)
		
		val Array(_trainData, _cvData) = data.randomSplit(Array(trainDataPart, 1 - trainDataPart))
		val trainData = _trainData.cache() 
		val cvData = _cvData.cache()

		val evaluations = for (
								impurity <- Array("gini");
								depth <- Array(20);
								bins <- Array(1000)
							) yield {
								println("-------------------->Goto train")
								
								val model = DecisionTree.trainClassifier(
									trainData, 2, categoricalFeaturesInfo,
									impurity, depth, bins)
								
								println("-------------------->Train is ended")

								val trainAccuracy = getMetrics(model, trainData).accuracy
								val cvAccuracy = getMetrics(model, cvData).accuracy
								
								if (trainDataPart >= 1.0)
									model.save(sparkContext, trainedModelsPath + f"$column-$impurity-$depth-$bins-with-params-binary")
								
								((impurity, depth, bins), (trainAccuracy, cvAccuracy))
							}

		evaluations.sortBy(_._2._2).reverse.foreach(println)
	} 

	def loadDecisionTree (sc : SparkContext, targetPointType : String, modelName : String) : DecisionTreeModel = {
		DecisionTreeModel.load(sc, f"$trainedModelsPath$targetPointType-$modelName")
	}

	def prediction(
			conf : SparkConf, 
			sparkContext : SparkContext, 
			sqlContext : SQLContext, 
			toPredictTransactions : RDD[Transaction], 
			targetPointType : String = "homePoint",
			modelName : String
		) : scala.collection.Map[String, Point] = {
		
		val model = loadDecisionTree(sparkContext, targetPointType, modelName)

		val prediction = toPredictTransactions.map{
				case t => 
					val point = t.transactionPoint
					val cityClusterID = TransactionPointCluster.getPointCluster(point).toDouble
					val countryID = TransactionClassifier.countriesCategories.get(t.country.getOrElse("")).get.toDouble
					val dayOfWeek = TransactionClassifier.getDateCategory(t.transactionDate.get).toDouble
					val operationType = t.operationType.toDouble
					val amount = t.amountPower10
					val currencyID = TransactionClassifier.currencyCategories.get(t.currency).get.toDouble

					val isPurpose = model.predict(Vectors.dense(Array(cityClusterID, countryID, dayOfWeek, operationType, amount, currencyID, point.getX, point.getY))).toInt
					(t.customer_id, point, isPurpose)
			}.
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
