package com.spark.raif.models

import org.apache.spark.rdd.RDD
import com.esri.core.geometry.Point

import com.spark.utils.StringUtils._
import com.spark.utils.geometry.PointUtils._
import com.spark.raif.Consts

object Shops {

	var shops = Map[(Int, Int), Map[Point, Map[Int, Double]]]()

	def init(trainTransactions : RDD[TrainTransaction], transactions : RDD[Transaction]) 
		: 
		Map[(Int, Int), Map[Point, Map[Int, Double]]] = {

		val mccStat = trainTransactions.
			map(t => t.transaction.mcc).
			countByValue.
			toMap

		val aroundHomeMccStat = trainTransactions.
			filter(
				t => t.homePoint.inRadios(t.transaction.point, Consts.scoreRadious)
			).
			map(t => t.transaction.mcc).
			countByValue.
			toMap

		val mccWeights = mccStat.map{case (k, v) => k -> (aroundHomeMccStat.getOrElse(k, 0l).toDouble / v) }.
			filter{case (k,v) => v > 0}.
			toMap

		shops = trainTransactions.
			map(t => (t.transaction.point, t.transaction.mcc, mccWeights.getOrElse(t.transaction.mcc, 0.0))).
			filter(_._3 > 0).
			union(
				transactions.
					map(t => (t.point, t.mcc, mccWeights.getOrElse(t.mcc, 0.0))).
					filter(_._3 > 0)
			).
			map(t => (t._1, (t._2, t._3))).
			groupByKey.
			mapValues{
				case pointMccIter => 
					pointMccIter.toMap
			}.
			map(t => (pointIndex(t._1) , t)).
			groupByKey.
			mapValues(t => t.toMap).
			collect.
			toMap

		shops
	}

	def pointIndex(point : Point) : (Int, Int) = {
		((point.getX / Consts.scoreRadious).floor.toInt, (point.getY / Consts.scoreRadious).floor.toInt)
	}

	def getPointWeight(point : Point, mccList : Set[Int]) : Double = {

		val xAxis = (point.getX / Consts.scoreRadious).floor.toInt
	    val yAxis = (point.getY / Consts.scoreRadious).floor.toInt

		(xAxis - 1 to xAxis + 1).flatMap(i =>
			(yAxis - 1 to yAxis + 1).flatMap(j =>
				
				shops.getOrElse((i, j), Map.empty).
					filter{
						case (p , mccMap) => point.inRadios(p, Consts.scoreRadious)
					}.
					map{
						case (p , mccMap) => 
							mccMap.
								filter{case (mcCode, weight) => !mccList.contains(mcCode)}.
								map{case (mcCode, weight) => weight}.
								sum
					}.
					toList
			).toList
		).sum
	}
}

