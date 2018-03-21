package com.lookuut

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext

import org.apache.spark.sql.functions._
import com.esri.core.geometry.Point
import scala.collection.mutable

import org.apache.spark.rdd._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.DenseVector

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SQLContext, Row, DataFrame, Column}

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, IndexToString} 
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.feature.{Bucketizer,Normalizer}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostClassificationModel}


object TransactionRegressor {
	def transactionVector(t : Transaction, ratio : Double) : org.apache.spark.ml.linalg.Vector = {
		val point = t.transactionPoint
		val cityClusterID = TransactionPointCluster.getPointCluster(t.transactionPoint).toDouble
		val countryID = TransactionClassifier.countriesCategories.get(t.country.getOrElse("")).get.toDouble
		val dayOfWeek = TransactionClassifier.getDateCategory(t.date.get).toDouble
		val operationType = t.operationType.toDouble
		val amount = t.amountPower10
		val currencyID = TransactionClassifier.currencyCategories.get(t.currency).get.toDouble
		val mcc = TransactionClassifier.mccCategories.get(t.mcc).get.toDouble
		
		org.apache.spark.ml.linalg.Vectors.dense(
			amount,
			currencyID, 
			cityClusterID, 
			countryID, 
			mcc,
			dayOfWeek, 
			operationType, 
			t.districtApartmensCount,
			ratio
		)
	}
}

class TransactionRegressor(private val sparkContext : SparkContext) {
	
	val sqlContext = new SQLContext(BankTransactions.spark.sparkContext) 
	import sqlContext.implicits._

	private val trainedModelsPath = "/home/lookuut/Projects/raif-competition/resource/models/"
	private val trainDataPart = 1.0


	def prepareTrainData (trainTransactions : RDD[TrainTransaction],
							column : String = "homePoint"
			) : RDD[(org.apache.spark.ml.linalg.Vector, Double, Double, String, Double, Double)] = {

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
									
									(
										TransactionRegressor.transactionVector(t.transaction, ratio), 
										if(isHome) 1.0 else 0.0, 
										if(isWork) 1.0 else 0.0, 
										t.transaction.customer_id,
										point.getX,
										point.getY
									)
							}
					}.flatMap(t => t)					
			}.
			flatMap(t => t)
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
	
	def train (
				trainTransactions : RDD[TrainTransaction],
				targetColumn : String = "homePoint"
			) : CrossValidatorModel = {

		val trainDF = prepareTrainData(trainTransactions, targetColumn).
									toDF("features", "homePoint", "workPoint",  "customer_id", "pointx", "pointy")


		val xgb = new XGBoostEstimator(get_param().toMap).
							setLabelCol(targetColumn).
							setFeaturesCol("features")
							// XGBoost paramater grid
		
		val xgbParamGrid = (new ParamGridBuilder()
		  .addGrid(xgb.round, Array(2))
		  .addGrid(xgb.maxDepth, Array(8))
		  .addGrid(xgb.maxBins, Array(2))
		  .addGrid(xgb.minChildWeight, Array(0.2))
		  .addGrid(xgb.alpha, Array(0.9))
		  .addGrid(xgb.lambda, Array(1.0))
		  .addGrid(xgb.subSample, Array(0.65))
		  .addGrid(xgb.eta, Array(0.015))
		  .build())

		val pipeline = new Pipeline().setStages(Array(xgb))

		val evaluator = (new BinaryClassificationEvaluator()
		  .setLabelCol(targetColumn)
		  .setRawPredictionCol("prediction")
		  .setMetricName("areaUnderROC"))

		val cv = (new CrossValidator()
		  .setEstimator(pipeline)
		  .setEvaluator(evaluator)
		  .setEstimatorParamMaps(xgbParamGrid)
		  .setNumFolds(5)
		)

	 	cv.fit(trainDF)
	} 

	def get_param(): scala.collection.mutable.HashMap[String, Any] = {
	    val params = new scala.collection.mutable.HashMap[String, Any]()
	        params += "eta" -> 0.1
	        params += "max_depth" -> 8
	        params += "gamma" -> 0.0
	        params += "colsample_bylevel" -> 1
	        params += "objective" -> "binary:logistic"
	        params += "num_class" -> 2
	        params += "booster" -> "gbtree"
	        params += "num_rounds" -> 20
	        params += "nWorkers" -> 3
	    return params
	}

	def generateModel(trainTransactions : RDD[TrainTransaction], 
						transactions : RDD[Transaction],
						targetColumn : String = "homePoint",
						trainDataPart : Double = 1.0) {
		
		

		val trainingFeaturesDF = prepareTrainData(trainTransactions, targetColumn).
									toDF("features", "homePoint", "workPoint",  "customer_id", "pointx", "pointy")

		val Array(trainDF, testDF) = trainingFeaturesDF.randomSplit(Array(1 - trainDataPart, trainDataPart), seed = 12345)
		val xgb = new XGBoostEstimator(get_param().toMap).
							setLabelCol(targetColumn).
							setFeaturesCol("features")
							// XGBoost paramater grid
		
		val xgbParamGrid = (new ParamGridBuilder()
		  .addGrid(xgb.round, Array(2))
		  .addGrid(xgb.maxDepth, Array(8))
		  .addGrid(xgb.maxBins, Array(2))
		  .addGrid(xgb.minChildWeight, Array(0.2))
		  .addGrid(xgb.alpha, Array(0.8, 0.9))
		  .addGrid(xgb.lambda, Array(0.9, 1.0))
		  .addGrid(xgb.subSample, Array(0.6, 0.65, 0.7))
		  .addGrid(xgb.eta, Array(0.015))
		  .build())

		val pipeline = new Pipeline().setStages(Array(xgb))

		val evaluator = (new BinaryClassificationEvaluator()
		  .setLabelCol(targetColumn)
		  .setRawPredictionCol("prediction")
		  .setMetricName("areaUnderROC"))

		val cv = (new CrossValidator()
		  .setEstimator(pipeline)
		  .setEvaluator(evaluator)
		  .setEstimatorParamMaps(xgbParamGrid)
		  .setNumFolds(5)
		)

		val xgbModel = cv.fit(trainDF)
		val results = xgbModel.transform(testDF)

		(xgbModel.bestModel.asInstanceOf[PipelineModel]
		  .stages(0).asInstanceOf[XGBoostClassificationModel]
		  .extractParamMap().toSeq.foreach(println))
		
		val prediction = results.
			select("probabilities", targetColumn, "customer_id", "prediction", "pointx", "pointy").
			map {
				case Row(
					probabilities: Vector, 
					targetColumn: Double, 
					customerId : String, 
					prediction: Double, 
					pointx : Double,
					pointy : Double
				) =>
				(customerId, (1 - probabilities(0), targetColumn, pointx, pointy))
			  }.
			  rdd.
			  groupByKey.
			  map {
			  	case (customerId, predictions) =>
			  		predictions.maxBy(_._1)._2
			  }.mean

		println("========> " +prediction)
	}


	def prepareTestData (transactions : RDD[Transaction]) : 
							RDD[(String, (Double, Double, org.apache.spark.ml.linalg.Vector))] = {

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
										
										(
											t.transactionPoint.getX,
											t.transactionPoint.getY, 
											TransactionRegressor.transactionVector(t, ratio)
										)
								}
						}.
						flatMap(t => t)

					(customer_id, trans)
			}.
			map(t => t._2.map(tt => (t._1, tt))).
			flatMap(t => t)
	}
	
	def prediction(
			toPredictTransactions : RDD[(String, (Double, Double, org.apache.spark.ml.linalg.Vector))], 
			targetColumn : String = "homePoint",
			model : CrossValidatorModel
		) : scala.collection.Map[String, Point] = {

		val sqlContext = new SQLContext(BankTransactions.spark.sparkContext) 
		import sqlContext.implicits._

		val testDF = toPredictTransactions.toDF("customer_id", "pointx", "pointy", "features")


		val results = model.transform(testDF)
		val prediction = results.
			select("probabilities", "customer_id", "prediction", "pointx", "pointy").
			map {
				case Row(
					probabilities: Vector, 
					customerId : String, 
					prediction: Double, 
					pointx : Double,
					pointy : Double
				) =>
				(customerId, (1 - probabilities(0), pointx, pointy))
			  }.
			  rdd.
			  groupByKey.
			  map {
			  	case (customerId, predictions) =>
			  		val maxProbabiltyPrediction = predictions.maxBy(_._1)
			  		(customerId, new Point(maxProbabiltyPrediction._2, maxProbabiltyPrediction._3))
			  }.
			  collectAsMap
		prediction
	}
}
