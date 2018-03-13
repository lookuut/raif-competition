package com.lookuut

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.esri.core.geometry.Point

import com.esri.dbscan.DBSCAN2
import com.esri.dbscan.DBSCANPoint

import org.apache.spark.rdd._

object TransactionPointCluster {

	private val clusterDistance = 0.3
	private val minPoint = 1
	private val roundBase = 100

	var clusters = scala.collection.Map[Point, Int]()
	var clustersCount = 0

	def clustering (transactions : RDD[Transaction], trainTransactions : RDD[TrainTransaction]) : scala.collection.Map[Point, Int] = {

		val points = transactions.
						map(t => t.transactionPoint).
						union(
							trainTransactions.
								map(t => t.transaction.transactionPoint)
						).
						map(roundPoint(_)).
						distinct.
						map(p => (p.getX.toString + p.getY.toString, p)).
						sortBy(_._1).
						zipWithUniqueId.
						map{case (t, index) => DBSCANPoint(index + 1, t._2.getX, t._2.getY)}.
						collect.
						toSeq
		
		clusters = DBSCAN2(clusterDistance, minPoint).
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

	def getClusters() : scala.collection.Map[Point, Int] = {
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