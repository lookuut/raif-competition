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
		dbscan(minDist, minPoint)
	}


	def trainDataAnalytics() {
		val conf = new SparkConf().
						setAppName(applicationName).
						setMaster("local[*]")		

		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

		import sqlContext.implicits._

		val transactionDataRaw = sparkContext.textFile(trainDataFile)
		
		println("================>")

		val transactions = transactionDataRaw.filter(!_.contains("amount,atm_address,")).
			zipWithIndex.
			map{
			case (line, index) => 
				Transaction.parseTrainTransaction(line, index)
			}.filter(x => x.transaction.city.getOrElse("") == "MOSCOW" && x.workPoint.getX > 0)
			.map {
				case(trainTransaction) => 
					val transaction = trainTransaction.transaction
					val (distance1, angle1) = cartesianToPolar(transaction.transactionPoint, moscowCartesianCenter)
					val (distance2, angle2) = cartesianToPolar(trainTransaction.workPoint, moscowCartesianCenter)
					val (distance3, angle3) = cartesianToPolar(trainTransaction.homePoint, moscowCartesianCenter)
					(transaction.amount, distance1, angle1, (distance2 * 1000))
			}.take(100).foreach(println)
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