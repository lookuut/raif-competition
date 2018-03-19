package com.lookuut.utils

import com.esri.core.geometry._

object RichPoint {
	def dot (point1 : Point, point2 : Point) = point1.getX * point2.getX + point1.getY * point2.getY
	def cross (point1 : Point, point2 : Point) = point1.getX * point2.getY - point1.getY * point2.getX
	def vector (point1 : Point, point2 : Point) = new Point(point1.getX - point2.getX, point1.getY - point2.getY)
}