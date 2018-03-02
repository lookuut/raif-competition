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

import org.apache.spark.rdd._

object Visualization {

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
		/*
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
		    saveAsTextFile(jsonResultFile)*/
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
		val point1 = new Point(3, 0)
		
		var mPoints = new MultiPoint()

		mPoints.add(0,0)
		mPoints.add(1,0)
		mPoints.add(4,0)
		
		val convexHull = OperatorConvexHull.local().execute(mPoints, null)
		
		println(!OperatorDisjoint.local().execute(point1, convexHull, null, null))
	}
}