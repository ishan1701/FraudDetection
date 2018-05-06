package CardFraudDetection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{ Vector, Vectors }

object Utils {
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
  // for DataFrame API
  val parseMerchnat = (merchant: String) => {
    try {
      if (merchant.contains("fraud_")) {
        val splitParts = merchant.split("_")
        splitParts(1)
      } else
        merchant
    } catch {
      case e: Exception =>
        println(e.printStackTrace())
        merchant
    }
  }
  def checkImbalancedCondition(df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession): (Boolean, Int, Int) = {
    import spark.implicits._
    val numOfPositiveRecords = df.filter($"is_fraud" === 0).count().toInt
    println(s"Nmber of positive is $numOfPositiveRecords")
    val positiveLabel = 0
    val numofNegetiveRecords = df.count().toInt - numOfPositiveRecords.toInt
    println(s"Nmber of negative is $numofNegetiveRecords")
    val negativeLabel = 1
    val totalPoints = numofNegetiveRecords + numOfPositiveRecords
    var numOFClusters = 0
    println(s"total records= $totalPoints")

    if ((numOfPositiveRecords / totalPoints) * 100 < 30) //(true,whichLabeltoReduce,numofClusters
    {
      println("I am in pos")

      (true, positiveLabel, numofNegetiveRecords)
    } else if (numofNegetiveRecords / totalPoints.toInt * 100 < 30) {
      println(" iam in Negaitev part")
      (true, negativeLabel, numOfPositiveRecords)
    } else
      (false, 0, 0)
  }

  //cc_numIndexed", "categoryIndexed", "merchantIndexed", "merch_lat", "merch_long","amt"
  def vectorToRDD(datapointsTuple: (Vector, Int)) = {
    val creditCard = datapointsTuple._1(0)
    val category = datapointsTuple._1(1)
    val merchant = datapointsTuple._1(2)
    val merchLat = datapointsTuple._1(3)
    val merchLon = datapointsTuple._1(4)
    val amount = datapointsTuple._1(5)
    val label = datapointsTuple._2
    Transaction(creditCard, category, merchant, merchLat, merchLon, amount, label)

  }
}