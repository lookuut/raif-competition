package com.esri.dbscan

object DBScanSpec extends org.specs2.mutable.Specification {

  "DBScan test" should {

    "Test clustering 2" in {

    	val points = Array(
    		DBSCANPoint(1, 0, 0), 
    		DBSCANPoint(2, -1, 0), 
    		DBSCANPoint(3, 1, 0), 
    		DBSCANPoint(4, 0, 1), 
    		DBSCANPoint(5, -1, 0.1),

    		DBSCANPoint(6, 0, 2), 
    		DBSCANPoint(7, -1, 3), 
    		DBSCANPoint(8, 1, 3), 
    		DBSCANPoint(9, 0, 3), 
    		DBSCANPoint(10, -1, 20)
    	)

		val clusters = DBSCAN2(1, 2).
			cluster(points)
		clusters.foreach(println)
		true
    }
  }
}



