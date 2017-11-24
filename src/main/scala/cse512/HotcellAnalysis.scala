package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._



object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*).cache()
    //pickupInfo.show()
    pickupInfo.createOrReplaceTempView("pickupInfo")
    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    //count the number of points in each cell
    var validPickupInfo = spark.sql("select x,y,z,count(*) as pointsMean,count(*) as pointsStdDev from pickupInfo where x>=" + minX.toString + " AND x<=" + maxX.toString + " AND y>=" + minY.toString + " AND y<=" + maxY.toString + " AND z>=" + minZ.toString + " AND z<=" + maxZ.toString + " GROUP BY x,y,z")
    //find the mean and standard deviation
    val meanSum1 = validPickupInfo.select(sum("pointsMean")).first().getLong(0)
    val meanSum = meanSum1.toDouble
    validPickupInfo = validPickupInfo.withColumn("pointsMean", lit(meanSum)/lit(numCells))
    validPickupInfo = validPickupInfo.withColumn("pointsStdDev2", col("pointsStdDev")*col("pointsStdDev"))
    var stdDev = validPickupInfo.select(sum("pointsStdDev2")).first().getLong(0)
    var stdDev2 = stdDev.toDouble / numCells
    stdDev2 = Math.sqrt(stdDev2 - ((meanSum/numCells)*(meanSum/numCells)))
    validPickupInfo = validPickupInfo.withColumn("pointsStdDev2", lit(stdDev2)).sort("x").sort("y").sort("z").persist()

    validPickupInfo.createOrReplaceTempView("validPickupInfo")
    //validPickupInfo.show()
    val subPickupInfo = spark.sql("select x as a,y as b ,z as c,pointsStdDev as p from validPickupInfo SORT BY x,y,z").persist()
    var iterCount = 0
    var filterDF = validPickupInfo
    // find all the neighbours

    filterDF = validPickupInfo.select("*").alias("table1").join(subPickupInfo.select("*").alias("table2"), (validPickupInfo("x") === subPickupInfo("a") -1 || validPickupInfo("x") === subPickupInfo("a") || validPickupInfo("x") === subPickupInfo("a") +1)
      && (validPickupInfo("y") === subPickupInfo("b") - 1 || validPickupInfo("y") === subPickupInfo("b") || validPickupInfo("y") === subPickupInfo("b") + 1)
      && (validPickupInfo("z") === subPickupInfo("c") -1 || validPickupInfo("z") === subPickupInfo("c") || validPickupInfo("z") === subPickupInfo("c") + 1)).persist()

    filterDF.createOrReplaceTempView("filterDF")

    spark.udf.register("CalNeighbours",(x: Int, y: Int, z:Int)=>((
      HotcellUtils.CalNeighbours(x,y,z)
      )))
    // calculate weighted sum of neighbours
    var joinDF = spark.sql("select x,y,z,first(pointsMean) as pointsMean, first(pointsStdDev2) as pointsStdDev2, sum(p) as neighbourVals, CalNeighbours(x,y,z) as noOfNeighbours from filterDF GROUP BY x,y,z")

    // calculate gscore
    joinDF = joinDF.withColumn("numerator",col("neighbourVals")-col("pointsMean")*col("noOfNeighbours"))
    joinDF = joinDF.withColumn("denominator",col("pointsStdDev2")*sqrt((col("noOfNeighbours")*numCells-col("noOfNeighbours")*col("noOfNeighbours"))/numCells-1))
    joinDF = joinDF.withColumn("GScore",col("numerator")/col("denominator")).persist()
    joinDF.createOrReplaceTempView("joinDF")
    val finalDF = spark.sql("select x,y,z from joinDF ORDER BY GScore DESC")
    // YOU NEED TO CHANGE THIS PART

    return finalDF // YOU NEED TO CHANGE THIS PART
  }
}