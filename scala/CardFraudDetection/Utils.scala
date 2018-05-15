package CardFraudDetection

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{ Vector, Vectors }

object Utils {
  def deleteOutputPath(config:com.typesafe.config.Config,fs:org.apache.hadoop.fs.FileSystem)={
    if(fs.exists(new Path(config.getString("modelPath"))))
      fs.delete(new Path(config.getString("modelPath")))
    if(fs.exists(new Path(config.getString("pipeLineModelPath"))))
      fs.delete(new Path(config.getString("pipeLineModelPath")))
        
  }
  val parseDateTime = (date: String, time: String) => {
    try {
      val finalDate = date.split(" +")(0) + " " + time
    
      finalDate
    } catch {
      case e: Exception =>
        println(e.printStackTrace())
        date
    }
  }
  def getDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
    val r : Int = 6371 //Earth radius
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(lon2 - lon1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }
  def checkImbalancedCondition(df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession): (Boolean, Int, Int) = {
    import spark.implicits._
    val numOfPositiveRecords = df.filter($"is_fraud" === 0).count().toInt
    val positiveLabel = 0
    val numofNegetiveRecords = df.count().toInt - numOfPositiveRecords.toInt
    val negativeLabel = 1
    val totalPoints = numofNegetiveRecords + numOfPositiveRecords
    var numOFClusters = 0
    if ((numOfPositiveRecords / totalPoints) * 100 < 30) //(true,whichLabeltoReduce,numofClusters
    {
      (true, positiveLabel, numofNegetiveRecords)
    } else if (numofNegetiveRecords / totalPoints.toInt * 100 < 30) {
      (true, negativeLabel, numOfPositiveRecords)
    } else
      (false, 0, 0)
  }
}