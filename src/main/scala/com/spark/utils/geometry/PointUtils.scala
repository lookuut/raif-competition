package com.spark.utils.geometry

import com.esri.core.geometry.Point

object PointUtils {
	implicit class RichPoint(val point: Point) {
		
		def distance(endPoint : Point) : Double = math.sqrt(squareDistance(endPoint))

		def squareDistance(endPoint : Point) : Double = {
			(point.getX - endPoint.getX) * (point.getX - endPoint.getX) 
			+ (point.getY - endPoint.getY) * (point.getY - endPoint.getY)
		}

		def inRadios(endPoint : Point, distance : Double) = squareDistance(endPoint) <= distance * distance
	}
}