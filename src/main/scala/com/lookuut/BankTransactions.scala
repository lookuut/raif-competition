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

import org.joda.time.DateTimeConstants
import org.apache.spark.storage.StorageLevel
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint
import java.io._

import org.apache.spark.rdd._

object BankTransactions {

	private val applicationName = "Bank transactions"
	private val dateFormat = "yyyy-MM-dd"

	private val testDataFile = "/home/lookuut/Projects/raif-competition/resource/test_set.csv"
	private val trainDataFile = "/home/lookuut/Projects/raif-competition/resource/train_set.csv"
	private val smallTestDataFile = "/home/lookuut/Projects/raif-competition/resource/small_test_set.csv"
	private val smallTrainDataFile = "/home/lookuut/Projects/raif-competition/resource/small_train_set.csv"
	private val myTestDataFile = "/home/lookuut/Projects/raif-competition/resource/my_test_set.csv"

	def main(args: Array[String]) {
		
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)

		val trainTransactions = sparkContext.
									textFile(trainDataFile).
									filter(!_.contains("amount,atm_address,")).
									zipWithIndex.
									map{
									case (line, index) => 
										Transaction.parseTrainTransaction(line, index)
									}.
									filter(t => !t.transaction.transactionDate.isEmpty)

		val transactions = sparkContext.
									textFile(testDataFile).
									filter(!_.contains("amount,atm_address,")).
									zipWithIndex.
									map{
									case (line, index) => 
										Transaction.parse(line, index)
									}.
									filter(t => !t.transactionDate.isEmpty)

		TrainTransactionsAnalytics.districts(conf, sparkContext, sqlContext, trainTransactions)
		return
		TransactionPointCluster.clustering(transactions, trainTransactions)							
		TransactionClassifier.getCountriesCategories(transactions, trainTransactions)
		TransactionClassifier.getCurrencyCategories(transactions, trainTransactions)
		TransactionClassifier.getMccCategories(transactions, trainTransactions)

		println(f"""=======> 
					${TransactionPointCluster.getClustersCount} 
					${TransactionClassifier.countriesCategories.size} 
					${TransactionClassifier.currencyCategories.size} 
					${TransactionClassifier.mccCategories.size} """)
		
		/*trainClassifier(conf, sparkContext, sqlContext, trainTransactions)

		return

		val modelName = "gini-20-1000-with-params-binary"		
		val predictedHomePoints = TransactionRegressor.prediction(conf, 
						sparkContext, 
						sqlContext, 
						transactions,
						"homePoint",
						modelName
					)

		val predictedWorkPoints = TransactionRegressor.prediction(conf, 
						sparkContext, 
						sqlContext, 
						transactions,
						"workPoint",
						modelName
					)
		val result = transactions.
						map(t => t.customer_id).
						distinct.
						map{
							case customer_id => 
								val workPoint = predictedWorkPoints.get(customer_id).getOrElse(new Point(0.0, 0.0))
								val homePoint = predictedHomePoints.get(customer_id).getOrElse(new Point(0.0, 0.0))
								(customer_id, workPoint.getX, workPoint.getY, homePoint.getX, homePoint.getY)
						}
		
		println(f"result count ${result.count} home points count ${result.filter(_._2 > 0).count} and ${result.filter(_._4 > 0).count}")

		import sqlContext.implicits._

		val df = result.toDF("_ID_", "_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_").cache()
		
		df.take(20).foreach(println)

		df.coalesce(1).write
		    .format("com.databricks.spark.csv")
		    .option("header", "true")
		    .save(f"/home/lookuut/Projects/raif-competition/resource/result$modelName")

		return */
		
		//trainClassifier(conf, sparkContext, sqlContext, trainTransactions)

		val modelName = "gini-20-800-with-params"
		val result = classifier(
						conf, 
						sparkContext, 
						sqlContext, 
						transactions, 
						trainTransactions,
						modelName
					).toSeq

		println("End of classifier")
		println(f"result count ${result.size} home points count ${result.filter(_._2 > 0).size} and ${result.filter(_._4 > 0).size}")

		import sqlContext.implicits._

		val df = (sparkContext.parallelize(result)).toDF("_ID_", "_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_").cache()
		
		df.take(20).foreach(println)

		df.coalesce(1).write
		    .format("com.databricks.spark.csv")
		    .option("header", "true")
		    .save(f"/home/lookuut/Projects/raif-competition/resource/result$modelName")
		/*
		val richTrainTrans = intersectWorkHomeCustomers.
		map{
			case (customer_id, homePoint) =>			
				val workPoint = calculatedWorkPointsMap.get(customer_id).get
				transactionMap.get(customer_id).get.map {
					case t => 
						new TrainTransaction(t, workPoint, homePoint)
				}
		}.
		flatMap(t => t)

		
		println(f"Riched train transaction count ${richTrainTrans.size}")

		val poorTransactions = transactions.filter(t => !intersectWorkHomeCustomers.contains(t.customer_id))
		println(f"Poor transactions count ${poorTransactions.count}")

		val richedTrainTransactions = trainTransactions.union(sparkContext.parallelize(richTrainTrans.toSeq)).repartition(8)
		trainClassifier(conf, sparkContext, sqlContext, richedTrainTransactions)

		val modelName = "entropy-20-300"
		val classified = classifier(
						conf, 
						sparkContext, 
						sqlContext, 
						poorTransactions, 
						richedTrainTransactions,
						modelName
					)

		val result = (classified ++ 
							intersectWorkHomeCustomers.
								map(t => (t._1,  
											calculatedWorkPointsMap.get(t._1).get.getX, 
											calculatedWorkPointsMap.get(t._1).get.getY, 
											t._2.getX, 
											t._2.getY
										)
							)).toSeq
							
		println(f"result count ${result.size} home points count ${result.filter(_._2 > 0).size} and ${result.filter(_._4 > 0).size}")

		import sqlContext.implicits._

		val df = (sparkContext.parallelize(result)).toDF("_ID_", "_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_").cache()
		
		df.take(20).foreach(println)

		df.coalesce(1).write
		    .format("com.databricks.spark.csv")
		    .option("header", "true")
		    .save(f"/home/lookuut/Projects/raif-competition/resource/result$modelName")*/
		    
		    
	}

	def characterPointDefine(conf : SparkConf, 
								sparkContext : SparkContext, 
								sqlContext : SQLContext, 
								transactions: RDD[Transaction],
								trainTransactions: RDD[TrainTransaction]) {
		for (
			pointEqualPercent <- Array(0.7, 0.9, 1.0);
			minPointEqualCount <- Array(3, 4)
		) yield {
		
			println(f"============> params $pointEqualPercent $minPointEqualCount")
			val calculatedHomePointsMap = TrainTransactionsAnalytics.featurePointIdentify(conf, sparkContext, sqlContext, transactions, trainTransactions, "homePoint", 0.9, pointEqualPercent, minPointEqualCount)
			val calculatedWorkPointsMap = TrainTransactionsAnalytics.featurePointIdentify(conf, sparkContext, sqlContext, transactions, trainTransactions, "workPoint", 0.9, pointEqualPercent, minPointEqualCount)

			val transactionMap = transactions.map(t => (t.customer_id, t)).groupByKey.collectAsMap

			val intersectWorkHomeCustomers = calculatedHomePointsMap.
												filter(t => 
														calculatedWorkPointsMap.contains(t._1) && 
														transactionMap.contains(t._1))

			println(f"""Intersect of work and home  ${intersectWorkHomeCustomers.size}""")
			////////////////////// add external customer ids
			val result = intersectWorkHomeCustomers.map {
				case (customer_id, homePoint) =>			
					val workPoint = calculatedWorkPointsMap.get(customer_id).get
					(customer_id , workPoint.getX, workPoint.getY, homePoint.getX, homePoint.getY)
			}.toSeq ++ transactions.
					filter(t => !intersectWorkHomeCustomers.contains(t.customer_id)).
					map(t => t.customer_id).
					distinct.
					map(t => (t, 0.0, 0.0, 0.0, 0.0)).
					collect.
					toSeq
			
			import sqlContext.implicits._
			val df = (sparkContext.parallelize(result)).toDF("_ID_", "_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_").cache()
			
			df.take(20).foreach(println)

			df.coalesce(1).write
			    .format("com.databricks.spark.csv")
			    .option("header", "true")
			    .save(f"/home/lookuut/Projects/raif-competition/resource/result-only-character-point-to-check-$pointEqualPercent-$minPointEqualCount")

		}
	}

	val homePointType = "homePoint"
	val workPointType = "workPoint"

	def trainClassifier (conf : SparkConf, 
								sparkContext : SparkContext, 
								sqlContext : SQLContext, 
								trainTransactions: RDD[TrainTransaction]) {
		
		TransactionClassifier.train(conf, sparkContext, sqlContext, trainTransactions, homePointType)
		TransactionClassifier.train(conf, sparkContext, sqlContext, trainTransactions, workPointType)
	}

	def classifier(conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext,
		transactions: RDD[Transaction],
		trainTransactions: RDD[TrainTransaction],
		modelName : String) : Iterable[(String, Double, Double, Double, Double)] = {
		
		val predictedHomePoints = TransactionClassifier.
									prediction(conf, 
												sparkContext, 
												sqlContext, 
												transactions, 
												trainTransactions.map(t => (t.homePoint, t)), 
												homePointType,
												modelName
											)	
		val predictedWorkPoints = TransactionClassifier.
									prediction(conf, 
												sparkContext, 
												sqlContext, 
												transactions, 
												trainTransactions.map(t => (t.workPoint, t)),
												workPointType,
												modelName
											)	
		
		predictedHomePoints.map{
			case (customer_id, homePoint) =>
				val workPoint = predictedWorkPoints.get(customer_id)
				val workPointX = if (workPoint.isEmpty) 0.0 else workPoint.get.getX
				val workPointY = if (workPoint.isEmpty) 0.0 else workPoint.get.getY
				(customer_id, workPointX, workPointY, homePoint.getX, homePoint.getY)
		}
	}
}