package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val arrRectLatLong=queryRectangle.split(",")
    val arrPoints = pointString.split(",")
    var maxLat=0.0
    var minLat=0.0
    var maxLong = 0.0
    var minLong = 0.0
    var lat1 = arrRectLatLong(0).toDouble
    var lat2 = arrRectLatLong(2).toDouble
    var long1 = arrRectLatLong(1).toDouble
    var long2 = arrRectLatLong(3).toDouble
    if (lat1>lat2) {
      maxLat = lat1
      minLat = lat2
    }
    else {
      maxLat = lat2
      minLat = lat1
    }
    if (long1>long2){
      maxLong = long1
      minLong = long2
    }
    else{
      maxLong = long2
      minLong = long1
    }
    var pointLat = arrPoints(0).toDouble
    var pointLong = arrPoints(1).toDouble

    if(pointLat>=minLat && pointLat<=maxLat && pointLong>=minLong && pointLong<=maxLong) {
      return true
    }

    return false

  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean ={
    val arrPoint1 = pointString1.split(",")
    val arrPoint2 = pointString2.split(",")
    var eucdDist=0.0
    eucdDist = (arrPoint1(0).toDouble-arrPoint2(0).toDouble)*(arrPoint1(0).toDouble-arrPoint2(0).toDouble) + (arrPoint1(1).toDouble-arrPoint2(1).toDouble)*(arrPoint1(1).toDouble-arrPoint2(1).toDouble)
    eucdDist = Math.sqrt(eucdDist)
    if(eucdDist<=distance)
      return true

    return false

  }

}
