package com.spark.raif.models


import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint
import com.esri.core.geometry.Point

import org.apache.spark.rdd.RDD
import com.spark.raif.Consts
import scala.collection.Map

object TransactionClusters {

	private val distance = Consts.scoreRadious * 3
	private val minPoint = 1
	private val roundBase = 100

	var clusters = scala.collection.Map[Point, Int]()
	var clustersCount = 0

	def clustering (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : Map[Point, Int] = {

		val points = transactions.
						map(t => t.point).
						union(
							trainTransactions.
								map(t => t.transaction.point)
						).
						map(roundPoint(_)).
						distinct.
						map(p => (p.getX.toString + p.getY.toString, p)).
						sortBy(_._1).
						zipWithUniqueId.
						map{case (t, index) => DBSCANPoint(index + 1, t._2.getX, t._2.getY)}.
						collect.
						toSeq
		
		clusters = DBSCAN2(distance, minPoint).
			cluster(
				points
			).
			map(p => (new Point(p.x, p.y), math.abs(p.clusterID))).
			toMap

		clustersCount = clusters.map(_._2).maxBy(t => t) + 1
		clusters
	}

	def getClustersCount() : Int = {
		clustersCount
	}

	def getClusters() : Map[Point, Int] = {
		clusters
	}

	def roundPoint(p : Point) : Point = {
		new Point(math.round(p.getX * roundBase).toDouble/roundBase, 
					math.round(p.getY * roundBase).toDouble/ roundBase)
	}

	def getPointCluster(point : Point) : Int = {
		clusters.get(roundPoint(point)).get
	}
}