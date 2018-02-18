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
import com.esri.core.geometry.{GeometryEngine, SpatialReference, Geometry}
import com.esri.core.geometry.Point
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ListBuffer
import org.joda.time.DateTimeConstants
import org.apache.spark.storage.StorageLevel
import scala.util.{Try,Success,Failure}


/**
 * Dataframe row class
 */

case class TrainTransaction(
	transaction : Transaction, 
	homePoint : Point,
	workPoint: Point
)

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
		run()
	}




}