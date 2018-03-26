package com.lookuut

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.esri.core.geometry.Point

class YandexMapJSONFormatter(private val sparkContext : SparkContext, private val jsonResultFile : String) {

	def cartesianToPolar(point : Point, cartesianCenter : Point) = {
		val cartesianPoint = new Point(point.getX - cartesianCenter.getX, 
										point.getY - cartesianCenter.getY)

		val distance : Double = math.sqrt(
			cartesianPoint.getX * cartesianPoint.getX + cartesianPoint.getY * cartesianPoint.getY
		)
		val angle : Double = math.atan2(cartesianPoint.getY, cartesianPoint.getX)
		
		(distance, angle)
	}
	
	def customerPoints (customerId : String, customerTransactions : Map[Point, (String, String)]) {

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
}