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


class NAStatCounter extends Serializable {
	val stats: StatCounter = new StatCounter()
	
	var missing: Long = 0
	var empty: Long = 0

	def add(x: Double): NAStatCounter = {
		if (java.lang.Double.isNaN(x)) {
			missing += 1
		} else {
			stats.merge(x)
		}
		this
	}

	def merge(other: NAStatCounter): NAStatCounter = {
		stats.merge(other.stats)
		missing += other.missing
		empty += other.empty
		this
	}

	override def toString = {
		"stats: " + stats.toString + " NaN: " + missing + " empty: " + empty
	}
}

object NAStatCounter extends Serializable {
	def apply(x: Double) = new NAStatCounter().add(x)
}

object BankTransactions {

	private val applicationName = "Bank transactions"
	private val dateFormat = "yyyy-MM-dd"

	private val testDataFile = "/home/lookuut/Projects/raif-competition/resource/test_set.csv"
	private val trainDataFile = "/home/lookuut/Projects/raif-competition/resource/train_set.csv"
	private val smallTestDataFile = "/home/lookuut/Projects/raif-competition/resource/small_test_set.csv"
	private val smallTrainDataFile = "/home/lookuut/Projects/raif-competition/resource/small_train_set.csv"
	private val myTestDataFile = "/home/lookuut/Projects/raif-competition/resource/my_test_set.csv"

	def main(args: Array[String]) {
		
		val minDist = if (args.size >= 1) args(0).toDouble else 0.0035
		val minPoint = if (args.size >= 2) args(1).toInt else 2
		
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
									}

		val transactions = sparkContext.
									textFile(testDataFile).
									filter(!_.contains("amount,atm_address,")).
									zipWithIndex.
									map{
									case (line, index) => 
										Transaction.parse(line, index)
									}
		//trainClassifier(conf, sparkContext, sqlContext)
		//classifier(conf, sparkContext, sqlContext)
		TrainTransactionsAnalytics.featurePointIdentify(conf, sparkContext, sqlContext, transactions, trainTransactions, "homePoint")
		TrainTransactionsAnalytics.featurePointIdentify(conf, sparkContext, sqlContext, transactions, trainTransactions, "workPoint")
		//trainDataAnalytics(conf, sparkContext, sqlContext)
		//testEsri
	}

	val homePointType = "homePoint"
	val workPointType = "workPoint"

	def trainClassifier (conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext) {
		
		val transactionDataRaw = sparkContext.textFile(trainDataFile)
		val trainTransactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.
			filter(t => t.transaction.transactionPoint.getX > 0).
			filter(t => t.transaction.city.get == "MOSCOW")

		TransactionClassifier.train(conf, sparkContext, sqlContext, trainTransactions, homePointType)
		TransactionClassifier.train(conf, sparkContext, sqlContext, trainTransactions, workPointType)
	}

	def classifier(conf : SparkConf, sparkContext : SparkContext, sqlContext : SQLContext) {
		
		val transactionDataRaw = sparkContext.textFile(testDataFile)
		
		val allTransactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parse(line, index)
			}.
			filter(t => t.transactionPoint.getX > 0)

		val transactions = allTransactions.
								filter(t => t.city.get == "MOSCOW").
								cache
		val trainTransactionDataRaw = sparkContext.textFile(trainDataFile)
		
		val trainTransactions = trainTransactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.
			filter(t => t.transaction.transactionPoint.getX > 0).
			filter(t => t.transaction.city.get == "MOSCOW")
		
		
		
		val notNskTransactions = allTransactions.
									map(t => (t.customer_id, t)).
									groupByKey.
									mapValues {
										case (values) => 
											val nskTransCount = values.filter(t => t.city.get == "NOVOSIBIRSK").size
											(nskTransCount)
									}.
									filter(_._2 == 0).
									map(t => (t._1, 1)).
									groupByKey.
									map{
										case (t) => 
											val r = scala.util.Random									
											(t._1, 55.32333 + r.nextFloat, 55.1222 + r.nextFloat, 54.2123 + r.nextFloat, 43.23123 + r.nextFloat)
									}.collect


		val trainHomePointTransactions = trainTransactions.map(t => (t.homePoint, t)).cache
		val trainWorkPointTransactions = trainTransactions.map(t => (t.workPoint, t)).cache

		val predictedHomePoints = TransactionClassifier.prediction(conf, sparkContext, sqlContext, transactions, trainHomePointTransactions, homePointType)	
		val predictedWorkPoints = TransactionClassifier.prediction(conf, sparkContext, sqlContext, transactions, trainWorkPointTransactions, workPointType)	
		
		val result = predictedHomePoints.map{
			case (customer_id, homePoint) =>
				val workPoint = predictedWorkPoints.get(customer_id)
				val workPointX = if (workPoint.isEmpty) 0.0 else workPoint.get.getX
				val workPointY = if (workPoint.isEmpty) 0.0 else workPoint.get.getY
				(customer_id, workPointX, workPointY, homePoint.getX, homePoint.getY)
		}.
		toSeq ++ notNskTransactions
		
		import sqlContext.implicits._

		val df = (sparkContext.parallelize(result)).toDF("_ID_", "_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_").cache()
		val customerCount = df.count
		
		println(f"Customers count $customerCount")
		df.take(20).foreach(println)

		df.coalesce(1).write
		    .format("com.databricks.spark.csv")
		    .option("header", "true")
		    .save("/home/lookuut/Projects/raif-competition/resource/result" + Calendar.getInstance().getTimeInMillis().toString)
	}
}