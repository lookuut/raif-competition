package com.lookuut

import com.esri.core.geometry.Point

@SerialVersionUID(1234L)
class TrainTransaction(
		val transaction : Transaction,
		val workPoint : Point,
		val homePoint : Point
	) extends Serializable {

	override def toString = f"""TrainTransaction([homePoint=$homePoint],[workPoint=$workPoint],[Transaction=$transaction])"""
}
