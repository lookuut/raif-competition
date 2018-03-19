package com.lookuut

import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.esri.core.geometry._

import org.apache.spark.storage.StorageLevel

import org.apache.spark.rdd._

class Visualization(private val sparkContext : SparkContext) {

	val moscowCartesianCenter = new Point(55.752818, 37.621754)

	private val jsonResultFile = "/home/lookuut/Projects/raif-competition/resource/result/dbscan/json/"
	private val csvResultFile = "/home/lookuut/Projects/raif-competition/resource/result/dbscan/csv"


	def cartesianToPolar(point : Point, cartesianCenter : Point) = {
		val cartesianPoint = new Point(point.getX - cartesianCenter.getX, 
										point.getY - cartesianCenter.getY)

		val distance : Double = math.sqrt(
			cartesianPoint.getX * cartesianPoint.getX + cartesianPoint.getY * cartesianPoint.getY
		)
		val angle : Double = math.atan2(cartesianPoint.getY, cartesianPoint.getX)
		
		(distance, angle)
	}
	
	def customerPoints (customerId : String, customerTransactions : scala.collection.Map[Point, (String, String)]) {

		val header = ("""{"type": "FeatureCollection","features": [""")
	    val bottom = ("""{"type": "Feature","id": -1, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},"options": {"preset": "islands#blueIcon"}}]}""")

	    val data = customerTransactions.
	      zipWithIndex.
	    map{
	    	case((p, label), index) =>  
	    		f"""{
	    				"type": "Feature",
	    				"id": ${index}, 
	    				"geometry": {
	    					"type": "Point", 
	    					"coordinates": [""" + p.getX + """, """ + p.getY + """]}, 
	    					"properties" :{ "hintContent" : """" + label._1 + """"}, 
	    					"options": {"preset": """" + label._2 + """"}
	    				},"""
		}.toSeq

		val withHeader = (header +: data) :+ bottom 

		sparkContext.
			parallelize(withHeader).
			coalesce(1).
		    saveAsTextFile(jsonResultFile + customerId)
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