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

import scala.collection.mutable.ListBuffer
import org.joda.time.DateTimeConstants
import org.apache.spark.storage.StorageLevel
import scala.util.{Try,Success,Failure}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint
import java.io._

import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._


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

	def run () {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

		import sqlContext.implicits._

		val transactionDataRaw = sparkContext.textFile(testDataFile)
		val transactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
								zipWithIndex.
								map{
								case (line, index) => 
									val t = Transaction.parse(line, index)
									(t.customer_id, t)
							}

		val tCount = transactions.count()
		
		println(f"Transactions count ========> [$tCount]")

		val result = transactions.groupByKey.mapValues{
			case(compactValues) => 
				
				val neirs = compactValues.map(item => (item.transactionPoint, item.id))
				val distances = compactValues.map{
					case(item) => 
						val distances = neirs.map {
							case(transactionPoint, id) => 
								val xDelta = item.transactionPoint.getX() - transactionPoint.getX()
								val yDelta = item.transactionPoint.getY() - transactionPoint.getY()
								val distance = xDelta * xDelta + yDelta * yDelta
								(item.id, id, distance)
						}.filter{case(id1, id2, distance) =>id1 != id2 && distance <= 0.0004}

						(item, distances.size, distances)
				} 
				distances
		}.map{
			case(customer_id, transactions) =>
				val workTransaction = Try(transactions.filter(t => {
					val dayOfWeek = if (t._1.transactionDate.isEmpty)
										DateTimeConstants.MONDAY
									else t._1.transactionDate.get.getDayOfWeek
					
					!(dayOfWeek == DateTimeConstants.SATURDAY
					 ||
					dayOfWeek == DateTimeConstants.SUNDAY)
				}).maxBy(_._2))

				
				val homeTransaction =Try(transactions.filter(t => {
					val dayOfWeek = if (t._1.transactionDate.isEmpty)
										DateTimeConstants.MONDAY
									else t._1.transactionDate.get.getDayOfWeek
					
					(dayOfWeek == DateTimeConstants.SATURDAY
					 ||
					dayOfWeek == DateTimeConstants.SUNDAY)
				}).maxBy(_._2))

				val workPoint = workTransaction match {
					case Success(point) => point._1.transactionPoint
					case Failure(message) => homeTransaction.get._1.transactionPoint
				}

				val homePoint = homeTransaction match {
					case Success(point) => point._1.transactionPoint
					case Failure(message) => workTransaction.get._1.transactionPoint
				}
				/*
				val homePoint = if (homeTransaction != null) homeTransaction._1.transactionPoint else workTransaction._1.transactionPoint
				val workPoint = if (workTransaction != null) workTransaction._1.transactionPoint else homeTransaction._1.transactionPoint
				*/
				(customer_id, 
					homePoint.getX, 
					homePoint.getY, 
					workPoint.getX, 
					workPoint.getY)
		}.persist()

		result.take(10).foreach(println(_))
		val df = result.toDF("_ID_", "_HOME_LAT_", "_HOME_LON_", "_WORK_LAT_", "_WORK_LON_").cache()

		df.coalesce(1).write
	    .format("com.databricks.spark.csv")
	    .option("header", "true")
	    .save("/home/lookuut/Projects/raif-competition/resource/result")
	}

	def main(args: Array[String]) {
		
		val minDist = if (args.size >= 1) args(0).toDouble else 0.0035
		val minPoint = if (args.size >= 2) args(1).toInt else 2
		
		trainClassifier2
		//dbscan(minDist, minPoint)
		//new EchoBot("536920770:AAFL4Sbpb66tsZep0v2r4hTALyoUy9Q_yIc").run()
	}

	def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]) : MulticlassMetrics = {
		val predictionsAndLabels = data.map(example =>
			(model.predict(example.features), example.label)
		)
		new MulticlassMetrics(predictionsAndLabels)
	}

	def trainClassifier2 () {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

		import sqlContext.implicits._
		// Load and parse the data file.
		val transactionDataRaw = sparkContext.textFile(trainDataFile)
		
		val trainTransactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.
			filter(t => t.transaction.transactionPoint.getX > 0 && t.transaction.city.get == "NOVOSIBIRSK").
			map(t => (t.homePoint.getX.toString + t.homePoint.getY.toString, t)).
			cache()

		val points = trainTransactions.
			groupByKey.
			mapValues{case(values) => values.head}.
			zipWithIndex.
			map{case((key, t), index) => 
				DBSCANPoint(index, t.homePoint.getX, t.homePoint.getY)
			}.collect()

		val clusteredPoints = DBSCAN2(0.01, 2).
								cluster(points).
								map(p => (math.abs(p.clusterID), new Point(p.x, p.y)))
		val maxClusterID = clusteredPoints.maxBy(_._1)._1 + 1

		val zeroClusterPoints = clusteredPoints.
			filter(_._1 == 0).
			zipWithIndex.
			map{
				case((clusterId, point), index) =>
					(index + maxClusterID, point)
			}

		val clusteredHomePoints = (zeroClusterPoints ++ clusteredPoints.filter(_._1 > 0)).
			map(t => (t._2.getX.toString + t._2.getY.toString, t._1)).
			toMap

		println("Clusters count =====>" + clusteredHomePoints.valuesIterator.toStream.distinct.size)

		val data = trainTransactions.
			groupByKey.
			mapValues{
				case groupedTrans =>
					var data = ListBuffer[Seq[Double]]()
					var buffer = ListBuffer[Double]()
					var index = 0
					val customerTransactions = groupedTrans.
						map{case (t) =>
							val tPoint = t.transaction.transactionPoint
							val hPoint = t.homePoint
							val x = (tPoint.getX - hPoint.getX) 
							val y = (tPoint.getY - hPoint.getY)
							val squaredDistance = x * x + y * y
							(squaredDistance, t)
						}

					val nearPurposePointTransactions = customerTransactions.
						filter(t => t._1 <= squareScoreRadious)
					
					if (nearPurposePointTransactions.size > 0) {
						scala.util.Random.shuffle(nearPurposePointTransactions).
							foreach {
								case (d, t) =>
									buffer += t.transaction.transactionPoint.getX
									buffer += t.transaction.transactionPoint.getY
									buffer += t.transaction.amount.get
									buffer += (if (t.transaction.atmPoint.getX > 0) 1 else 0).toDouble
									index += 1

									if (index >= 5) {
										data += buffer.toSeq
										buffer = ListBuffer[Double]()
										index = 0
									}
							}
					} else {
						val t = customerTransactions.toSeq.sortWith(_._1 > _._1).head._2
						buffer += t.transaction.transactionPoint.getX
						buffer += t.transaction.transactionPoint.getY
						buffer += t.transaction.amount.get
						buffer += (if (t.transaction.atmPoint.getX > 0) 1 else 0).toDouble
					}

					if (buffer.size > 0) {
						val lastPos = buffer.size
						for (i <- buffer.size / 4 to 5 if i < 5) {
							buffer += buffer(lastPos - 4)
							buffer += buffer(lastPos - 3)
							buffer += buffer(lastPos - 2)
							buffer += buffer(lastPos - 1)
						}	
						data += buffer.toSeq
					}
					
					data.toSeq
			}.
			flatMap{case (sPoint, values) => values.map((sPoint, _))}.
			map{
				case (sPoint, params) => 
					LabeledPoint(
						clusteredHomePoints.get(sPoint).get.toDouble,
						Vectors.dense(
							params.toArray
						)
					)
			}

		val Array(_trainData, _cvData) = data.randomSplit(Array(0.8, 0.2))
		val trainData = _trainData.cache() 
		val cvData = _cvData.cache()
		// Train a DecisionTree model.
		//  Empty categoricalFeaturesInfo indicates all features are continuous.
		val categoricalFeaturesInfo = Map[Int, Int]()

		val evaluations = for (
								impurity <- Array("gini", "entropy");
								depth <- Array(10, 20, 30);
								bins <- Array(40, 100, 300)
							) yield {
								println(f"=============> Smart of $impurity, $depth, $bins")
								val model = DecisionTree.trainClassifier(
								trainData, clusteredHomePoints.valuesIterator.max.toInt + 1, categoricalFeaturesInfo,
								impurity, depth, bins)
								println(f"=============> End of $impurity, $depth, $bins")

								val trainAccuracy = getMetrics(model, trainData).precision
								val cvAccuracy = getMetrics(model, cvData).precision
								((impurity, depth, bins), (trainAccuracy, cvAccuracy))
							}

		evaluations.sortBy(_._2).reverse.foreach(println)
		
	} 

	def trainClassifier () {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

		import sqlContext.implicits._
		// Load and parse the data file.
		val transactionDataRaw = sparkContext.textFile(trainDataFile)
		
		val trainTransactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.
			filter(t => t.transaction.transactionPoint.getX > 0).
			filter{case (t) => 
							val tPoint = t.transaction.transactionPoint
							val hPoint = t.homePoint
							val x = (tPoint.getX - hPoint.getX) 
							val y = (tPoint.getY - hPoint.getY)
							val distance = x * x + y * y
							distance * distance <= squareScoreRadious
			}.cache()

		val homePointMap = trainTransactions.
			map(t => (t.homePoint.getX.toString + t.homePoint.getY.toString, 1)).
			groupByKey.
			zipWithUniqueId.
			map{case((key, value), index) => (key, index)}.
			collectAsMap()

		val cityMap = trainTransactions.
			map(t => (t.transaction.city, 1)).
			groupByKey.
			zipWithUniqueId.
			map{case((key, value), index) => (key, index)}.
			collectAsMap()

		val mccMap = trainTransactions.
			map(t => (t.transaction.mcc, 1)).
			groupByKey.
			zipWithUniqueId.
			map{case((key, value), index) => (key, index)}.
			collectAsMap()

	
		println("==========>")
		println(homePointMap.size)
		println(cityMap.size)
		println(mccMap.size)
		
		println("==========>")

		val data = trainTransactions.
			map{
				case (t) => 
					LabeledPoint(
						homePointMap.get(t.homePoint.getX.toString + t.homePoint.getY.toString).get.toDouble,
						Vectors.dense(
							Array(
								//cityMap.get(t.transaction.city).get.toDouble,
								//mccMap.get(t.transaction.mcc).get.toDouble,
								t.transaction.transactionPoint.getX,
								t.transaction.transactionPoint.getY,
								t.transaction.amount.getOrElse(0.0)
							)
						)
					)
			}.cache()

		val Array(_trainData, _cvData) = data.randomSplit(Array(0.7, 0.3))
		val trainData = _trainData.cache() 
		val cvData = _cvData.cache()
		// Train a DecisionTree model.
		//  Empty categoricalFeaturesInfo indicates all features are continuous.
		val categoricalFeaturesInfo = Map/*(
											0 -> cityMap.size, 
											1 -> mccMap.size
										)*/[Int, Int]()

		val evaluations = for (
								impurity <- Array("gini", "entropy");
								depth <- Array(30);
								bins <- Array(300)
							) yield {
								println(f"=============> Smart of $impurity, $depth, $bins")
								val model = DecisionTree.trainClassifier(
								trainData, homePointMap.valuesIterator.max.toInt + 1, categoricalFeaturesInfo,
								impurity, depth, bins)
								println(f"=============> End of $impurity, $depth, $bins")

								val trainAccuracy = getMetrics(model, trainData).precision
								val cvAccuracy = getMetrics(model, cvData).precision
								((impurity, depth, bins), (trainAccuracy, cvAccuracy))
							}

		evaluations.sortBy(_._2).reverse.foreach(println)
	}

	private val scoreRadious = 0.02
	private val squareScoreRadious = 0.0004

	def trainDataAnalytics() {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

		import sqlContext.implicits._

		val transactionDataRaw = sparkContext.textFile(trainDataFile)
		
		val transactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.
			filter(t => t.transaction.transactionPoint.getX > 0).
			cache()

		println("===>Train data count " + transactions.count())
		val customerCount = transactions.map(_.transaction.customer_id).distinct().count()
		val customersWithWorkCount = transactions.filter(_.workPoint.getX > 0).map(_.transaction.customer_id).distinct().count()
		println("===>Customer count " + customerCount)
		println("===>Customers with work count " + customersWithWorkCount)

		val customers = transactions.
			map(t => (t.transaction.customer_id, t)).
			groupByKey.
			mapValues{
				case (customerTrans) => 
					val nearHomePointsCount = customerTrans.
						filter{case (t) => 
								val tPoint = t.transaction.transactionPoint
								val hPoint = t.homePoint
								val x = (tPoint.getX - hPoint.getX) 
								val y = (tPoint.getY - hPoint.getY)
								val distance = x * x + y * y
								distance * distance <= squareScoreRadious
							}.size

					val nearWorkPointsCount = customerTrans.
						filter{
							case(t) => 
								val tPoint = t.transaction.transactionPoint
								val hPoint = t.workPoint
								val x = (tPoint.getX - hPoint.getX) * (tPoint.getX - hPoint.getX)
								val y = (tPoint.getY - hPoint.getY) * (tPoint.getY - hPoint.getY)
								val distance = x + y
								distance * distance <= squareScoreRadious
							}.size
					val pointsCount = customerTrans.size
					(pointsCount, nearHomePointsCount.toDouble / pointsCount, nearWorkPointsCount.toDouble / pointsCount.toDouble, nearHomePointsCount, nearWorkPointsCount)
			}.cache()

		val home10 = customers.filter(_._2._4 >= 10).count().toDouble / customerCount 
		val home8 = customers.filter(_._2._4 >= 8).count().toDouble / customerCount
		val home5 = customers.filter(_._2._4 >= 5).count().toDouble / customerCount

		val work10 = customers.filter(_._2._5 >= 10).count().toDouble / customersWithWorkCount
		val work8 = customers.filter(_._2._5 >= 8).count().toDouble / customersWithWorkCount
		val work5 = customers.filter(_._2._5 >= 5).count().toDouble / customersWithWorkCount

		println(f"===> home10 $home10, home8 $home8, home5 $home5 and work10 $work10, work8 $work8, work5 $work5")
	}

	private val moscowCartesianCenter = new Point(55.752818, 37.621754)

	def cartesianToPolar(point : Point, cartesianCenter : Point) = {
		val cartesianPoint = new Point(point.getX - cartesianCenter.getX, 
										point.getY - cartesianCenter.getY)

		val distance : Double = math.sqrt(
			cartesianPoint.getX * cartesianPoint.getX + cartesianPoint.getY * cartesianPoint.getY
		)
		val angle : Double = math.atan2(cartesianPoint.getY, cartesianPoint.getX)
		
		(distance, angle)
	}
	
	private val jsonResultFile = "/home/lookuut/Projects/raif-competition/resource/result/dbscan/json"
	private val csvResultFile = "/home/lookuut/Projects/raif-competition/resource/result/dbscan/csv"

	def dbscan(minDist : Double, minPoint : Int) = {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)

		import sqlContext.implicits._

		val testDataRaw = sparkContext.textFile(testDataFile)
		val parsedTrainData = sparkContext.
				textFile(trainDataFile).
				filter(!_.contains("amount,atm_address,")).
				map{
					case(line) => 
						Transaction.parseTrainTransaction(line, 0)
				}.cache()

		val points = testDataRaw.
			filter(!_.contains("amount,atm_address,")).
			map{//Test data transactions
				case (line) => 
					val t = Transaction.parse(line, 0)
				(t.transactionPoint.getX.toString + t.transactionPoint.getY.toString, t.transactionPoint)
			}.union(
				parsedTrainData.
					map(t =>
							(
								t.transaction.transactionPoint.getX.toString +
							 		t.transaction.transactionPoint.getY.toString, 
						 		t.transaction.transactionPoint
						 	)
					)
			).union(
				parsedTrainData.
					map(t =>
							(
								t.workPoint.getX.toString +
									t.workPoint.getY.toString, 
						 		t.workPoint
						 	)
						)
			).union(
				parsedTrainData.
					map(t => 
							(
								t.homePoint.getX.toString +
							 		t.homePoint.getY.toString, 
						 		t.homePoint
							)
						)
			).
			groupByKey.
			mapValues{case(values) => values.head}.
			zipWithIndex.
			map{case((key, point), index) => 
				DBSCANPoint(index, point.getX, point.getY)
			}.collect()

		val clusteredPoints = DBSCAN2(minDist, minPoint).
								cluster(points).
								map(p => (math.abs(p.clusterID), p.x, p.y)).
								toSeq.
								sortWith(_._1 > _._1)		
		
		sparkContext.
			parallelize(clusteredPoints).
			toDF("cluster", "latitude", "longtitude").
			coalesce(1).
			write.
		    format("com.databricks.spark.csv").
		    option("header", "true").
		    save(csvResultFile)


	    val header = ("""{"type": "FeatureCollection","features": [""")
	    val bottom = ("""{"type": "Feature","id": -1, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},"options": {"preset": "islands#blueIcon"}}]}""")

	    val data = clusteredPoints.
	      zipWithIndex.
	    map{
	    	case(p, index) =>  
	    		f"""{"type": "Feature","id": $index, "geometry": {"type": "Point", "coordinates": [""" + p._2 + """, """ + p._3 + """]}, "properties" :{ "hintContent" : "Cluster """ + p._1 + """"}, "options": {"preset": """" + colors(p._1  % 10)  + """"}},"""
		}.toSeq

		val withHeader = (header +: data) :+ bottom 

		sparkContext.
			parallelize(withHeader).
			coalesce(1).
		    saveAsTextFile(jsonResultFile)
	}

	private val colors = Array(
		"islands#blueIcon", 
		"islands#redIcon", 
		"islands#darkOrangeIcon", 
		"islands#nightIcon", 
		"islands#darkBlueIcon", 
		"islands#pinkIcon", 
		"islands#grayIcon", 
		"islands#brownIcon", 
		"islands#darkGreenIcon", 
		"islands#violetIcon", 
		"islands#blackIcon"
	)

	def testEsri() {
		val  point = new Point(-10, -10);

		val  point1 = new Point(1, 1);
		val  point2 = new Point(2, 2);

		var polygon = new Polygon();
		polygon.startPath(0, 0);
		polygon.lineTo(3, 3);
		polygon.lineTo(0, 3);
		polygon.lineTo(3, 0);
		
		println(OperatorDisjoint.local().execute(point, polygon, null, null))
		println(OperatorDisjoint.local().execute(point1, polygon, null, null))
		println(OperatorDisjoint.local().execute(point2, polygon, null, null))
	}
}