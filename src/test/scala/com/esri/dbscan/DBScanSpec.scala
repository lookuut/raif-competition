package com.esri.dbscan

object DBScanSpec extends org.specs2.mutable.Specification {

  "DBScan test" should {

    "Test clustering 2" in {
        val eps = 1.0
    	val points = Array(
    		DBSCANPoint(1, 0, 0), 
    		DBSCANPoint(2, 1, 0), 
    		DBSCANPoint(3, -1, 0),
            DBSCANPoint(4, -2, 0)
    	)

		val clusters = DBSCAN2(eps, 3).
			cluster(points)

        val left = Map(0 -> "0", 1 -> "1", 2 -> "2")
        
        val right = Map(3 -> "3", 4 -> "4", 5 -> "5")

        val merge = left ++ right

        println(merge)
		
        true
    }
  }
}



