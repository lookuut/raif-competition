package com.esri.dbscan

object DBScanSpec extends org.specs2.mutable.Specification {

  "DBScan test" should {

    "Test clustering 2" in {
        val eps = 1.0
    	val points = Array(
    		DBSCANPoint(1, 0, 0), 
    		DBSCANPoint(2, 1, 0), 
    		DBSCANPoint(3, -21, 0),
            DBSCANPoint(4, 21, 0), 
            DBSCANPoint(5, -21, 0),
            DBSCANPoint(6, 21, 0), 
            DBSCANPoint(7, -21, 0),
            DBSCANPoint(8, -22, 0)
    	)

		val clusters = DBSCAN2(eps, 1).
			cluster(points).
            map(p => (p.clusterID, p)).
            groupBy(_._1).
            foreach(println)
		
        true
    }
  }
}



