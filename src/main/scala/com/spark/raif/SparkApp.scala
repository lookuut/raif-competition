package com.spark.raif

import com.esri.core.geometry.Point

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.spark.raif.models._
import com.spark.raif.solutions._
import com.spark.raif.transformers._


object SparkApp {
	
	val spark = SparkSession.builder().getOrCreate()
	val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd-hh-mm-ss")

	import spark.implicits._

	val resourceDir = Config.appDir + "resource/"
	val modelsDir = resourceDir + "models/"

	private val predictionsDir = resourceDir + "predictions/"
	private val applicationName = "raif-competition"
	
	private val testDataFile = resourceDir + "test_set.csv"
	private val trainDataFile = resourceDir + "train_set.csv"
	
	private val smallTestDataFile = resourceDir + "small_test_set.csv"
	private val smallTrainDataFile = resourceDir + "small_train_set.csv"
	private val apartmensDataFile = resourceDir + "ru-apartmens.csv"

	def main(args: Array[String]) {
		Apartment.init(loadApartments, Consts.scoreRadious)
		
		val (trainTransactions, transactions) = loadTransactions(smallTrainDataFile, smallTestDataFile)
		val shops = Shops.init(trainTransactions, transactions)
		println(f"${shops.size} ${Shops.shops.size}")
		
		binaryLogistic(trainTransactions, transactions)
	}

	def binaryLogistic(trainTransactions : RDD[TrainTransaction], 
										transactions : RDD[Transaction]) {

		val categoricalFeatures = CategoricalFeatures.init(trainTransactions, transactions)
		
		val features = new PointFeatures(spark, categoricalFeatures)
		val binaryLogistic = new XGBoostBinaryLogistic(spark)
		
		val models = (for (targetPoint <- Array(TrainTransaction.homePoint, TrainTransaction.workPoint)) yield {
			val trainData = features.prepareTransactions(trainTransactions.
															map(t => 
																(
																	t.transaction, 
																	Some(t.getTargetPoint(targetPoint))
																)
															)
														)
			
			println("Point count " + trainData.count + "/" + trainData.first._4.size)
			
			(
				targetPoint, 
				binaryLogistic.generateModel(trainData.map(t => (t._1, t._3, t._4)), targetPoint, 0.7)
			)
		}).toMap

		val modelHome = binaryLogistic.loadModel(models.get(TrainTransaction.homePoint).get)
		val modelWork = binaryLogistic.loadModel(models.get(TrainTransaction.workPoint).get)

		val testTransactions = transactions.
									map(t => 
										(
											t, 
											Option.empty[Point]
										)
									)

		val prepredTestTransactions = features.prepareTransactions(testTransactions)
		val predictedHomePoints = binaryLogistic.prediction(prepredTestTransactions, modelHome)
		val predictedWorkPoints = binaryLogistic.prediction(prepredTestTransactions, modelWork)

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


		val df = spark.sparkContext.parallelize(result.collect.toSeq).
						toDF("_ID_","_WORK_LAT_", "_WORK_LON_", "_HOME_LAT_", "_HOME_LON_")

		df.coalesce(1).write
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.save(f"${predictionsDir}xgboost-resuls${SparkApp.dateFormat.print(DateTime.now)}")
	}

	def loadApartments() : RDD[Apartment] = {
		SparkApp.spark.sparkContext.
				textFile(apartmensDataFile).
				filter(!_.contains("latitude,longitude,year")).
				zipWithIndex.
				map{
					case (line, index) => 
						Apartment.parse(line, index)
				}
	}

	def loadTransactions(trainFile : String, testFile : String) : (RDD[TrainTransaction], RDD[Transaction]) = {
		val trainTransactions = spark.sparkContext.
								textFile(trainFile).
								filter(!_.contains("amount,atm_address,")).
								zipWithIndex.
								map{
								case (line, id) => 
									TrainTransaction.parse(line, id)
								}.
								filter(t => t.transaction.point.getX > 0).
								filter(t => !t.transaction.date.isEmpty)
	
		val transactions = spark.sparkContext.
							textFile(testFile).
							filter(!_.contains("amount,atm_address,")).
							zipWithIndex.
							map{
							case (line, index) => 
								Transaction.parse(line, index)
							}.
							filter(t => t.point.getX > 0).
							filter(t => !t.date.isEmpty)

		(trainTransactions, transactions)
	}

	def characterPointDefine(transactions: RDD[Transaction],
								trainTransactions: RDD[TrainTransaction],
								categoricalFeatures : CategoricalFeatures
							) : Map[String, Map[String, Point]] = {
		
		(for (targetPoint <- Array(
									TrainTransaction.homePoint, 
									TrainTransaction.workPoint
								)
					) yield 
		{
			(
				targetPoint 
				-> 
				CustomerPointFeatures.
					perform(
						transactions, 
						trainTransactions, 
						targetPoint, 
						0.9, 
						1.0, 
						4
					)
			)
		}).toMap
	}
}