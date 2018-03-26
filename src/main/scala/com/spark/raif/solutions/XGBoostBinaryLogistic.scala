package com.spark.raif.solutions

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.esri.core.geometry.Point

import org.apache.spark.rdd.RDD

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator, CrossValidatorModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostClassificationModel}

import scala.collection.mutable.HashMap

import com.spark.raif.SparkApp

class XGBoostBinaryLogistic(private val spark : SparkSession) {
	
	import spark.implicits._

	private val trainDataPart = 1.0

	def train (
				trainData : RDD[(String, Double, Vector)],
				lambda : Double
			) : CrossValidatorModel = {

		val trainDF = trainData.toDF("customer_id", "label", "features")

		val xgb = new XGBoostEstimator(get_param().toMap).
							setLabelCol("label").
							setFeaturesCol("features")
							// XGBoost paramater grid
		
		val xgbParamGrid = (new ParamGridBuilder()
		  .addGrid(xgb.round, Array(100))
		  .addGrid(xgb.maxDepth, Array(10))
		  .addGrid(xgb.maxBins, Array(20))
		  .addGrid(xgb.minChildWeight, Array(0.2))
		  .addGrid(xgb.alpha, Array(0.8))
		  .addGrid(xgb.lambda, Array(lambda))
		  .addGrid(xgb.subSample, Array(0.65))
		  .addGrid(xgb.eta, Array(0.006))
		  .build())

		val pipeline = new Pipeline().setStages(Array(xgb))

		val evaluator = (new BinaryClassificationEvaluator()
		  .setLabelCol("label")
		  .setRawPredictionCol("prediction")
		  .setMetricName("areaUnderROC"))

		val cv = (new CrossValidator()
		  .setEstimator(pipeline)
		  .setEvaluator(evaluator)
		  .setEstimatorParamMaps(xgbParamGrid)
		  .setNumFolds(2)
		)

	 	val model = cv.fit(trainDF)
	 	
	 	model.save(SparkApp.modelsDir + f"xgboost-lambda:$lambda" + SparkApp.dateFormat.print(DateTime.now))
	 	model
	} 
	
	def get_param(): HashMap[String, Any] = {
	    val params = new HashMap[String, Any]()
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

	def generateModel(trainData : RDD[(String, Double, Vector)], 
						column : String,
						trainDataPart : Double = 1.0) : String = {

		val trainingFeaturesDF = trainData.
									toDF("customer_id", "label", "features")
		
		val Array(train, test) = trainingFeaturesDF.
										randomSplit(Array(1 - trainDataPart, trainDataPart))
		
		val trainDF = train.repartition(16)
		val testDF = test.repartition(16)

		val xgb = new XGBoostEstimator(get_param().toMap).
							setLabelCol("label").
							setFeaturesCol("features")
							// XGBoost paramater grid
		
		val xgbParamGrid = (new ParamGridBuilder()
		  .addGrid(xgb.round, Array(10))
		  .addGrid(xgb.maxDepth, Array(5))
		  .addGrid(xgb.maxBins, Array(20))
		  .addGrid(xgb.minChildWeight, Array(0.1))
		  .addGrid(xgb.alpha, Array(0.8))
		  .addGrid(xgb.lambda, Array(0.8))
		  .addGrid(xgb.subSample, Array(0.6))
		  .addGrid(xgb.eta, Array(0.006))
		  .build())

		val pipeline = new Pipeline().setStages(Array(xgb))

		val evaluator = (new BinaryClassificationEvaluator()
		  .setLabelCol("label")
		  .setRawPredictionCol("prediction")
		  .setMetricName("areaUnderROC"))

		val cv = (new CrossValidator()
		  .setEstimator(pipeline)
		  .setEvaluator(evaluator)
		  .setEstimatorParamMaps(xgbParamGrid)
		  .setNumFolds(3)
		)

		val xgbModel = cv.fit(trainDF.cache)
		val bestModelPath = SparkApp.modelsDir + f"xgboost-$column-" + SparkApp.dateFormat.print(DateTime.now)
		xgbModel.save(bestModelPath)

		val results = xgbModel.transform(testDF.cache)

		(xgbModel.bestModel.asInstanceOf[PipelineModel]
		  .stages(0).asInstanceOf[XGBoostClassificationModel]
		  .extractParamMap().toSeq.foreach(println))
		
		val prediction = results.
			select("probabilities", "customer_id", "prediction", "label").
			map {
				case Row(
					probabilities: Vector, 
					customer_id : String, 
					prediction: Double,
					label : Double
				) =>
				(customer_id, (1 - probabilities(0), label))
			  }.
			  rdd.
			  groupByKey.
			  map {
			  	case (customerId, predictions) =>
			  		predictions.maxBy(_._1)._2
			  }.mean

		println("========> " + prediction)
		bestModelPath
	}
	
	def prediction(
			toPredictTransactions : RDD[(String, Point, Double, Vector)], 
			model : CrossValidatorModel
		) : scala.collection.Map[String, Point] = {


		val testDF = toPredictTransactions.
						map(t => (t._1, t._2.getX, t._2.getY, t._4)).
						toDF("customer_id", "pointx", "pointy", "features").
						cache


		val results = model.transform(testDF)
		results.
			select("probabilities", "customer_id", "prediction", "pointx", "pointy").
			map {
				case Row(
					probabilities: Vector, 
					customer_id : String, 
					prediction: Double, 
					pointx : Double,
					pointy : Double
				) =>
				(customer_id, (1 - probabilities(0), pointx, pointy))
			  }.
			  rdd.
			  groupByKey.
			  map {
			  	case (customerId, predictions) =>
			  		val maxProbabiltyPrediction = predictions.maxBy(_._1)
			  		(customerId, new Point(maxProbabiltyPrediction._2, maxProbabiltyPrediction._3))
			  }.
			  collectAsMap
	}

	def loadModel(path : String) : CrossValidatorModel = {
		CrossValidatorModel.load(path)
	}
}
