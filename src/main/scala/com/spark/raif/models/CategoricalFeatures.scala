/**
 *
 *
 * @author Lookuut Struchkov
 * @desc Transcation categorical features for one hot encoding
 * 	
 *
 */

package com.spark.raif.models

import org.apache.spark.rdd.RDD

object CategoricalFeatures {
	def init(trainTransactions : RDD[TrainTransaction], 
						transactions : RDD[Transaction]) : CategoricalFeatures = {

		val countriesCategories = trainTransactions.
				map(t => t.transaction.country.getOrElse("")).
				union(
					transactions.map(t => t.country.getOrElse(""))
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (country, index) => (country, index)}.
				collect.
				toMap

		val mccCategories = trainTransactions.
				map(t => t.transaction.mcc).
				union(
					transactions.map(t => t.mcc)
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (mcc, index) => (mcc, index)}.
				collect.
				toMap

		val currencyCategories = trainTransactions.
				map(t => t.transaction.currency).
				union(
					transactions.map(t => t.currency)
				).
				distinct.
				sortBy(t => t).
				zipWithIndex.
				map{case (currency, index) => (currency, index)}.
				collect.
				toMap

		new CategoricalFeatures(countriesCategories, currencyCategories, mccCategories)
	}
}

class CategoricalFeatures(private val countriesCategories : Map[String, Long],
					private val currencyCategories : Map[Int, Long],
					private val mccCategories : Map[Int, Long]) extends Serializable {

	def getCountriesCategories() = countriesCategories
	def getCurrencyCategories() = currencyCategories
	def getMccCategories() = mccCategories
}