package CardFraudDetection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.log4j
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ParseTransactionData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Detection")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val logger=Logger.getLogger(ParseTransactionData.getClass)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    import spark.implicits._
    try {
      val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> "|")
      val readPath = "G:\\Ishan\\MachineLeaning\\Pramod Data\\raw_transaction.csv"
      val transactionData = spark.read.options(readOption).csv(readPath)
      transactionData.printSchema()
      val parseMerhantUDF = udf(Utils.parseMerchnat)
      val parsedTransaction = transactionData.withColumn("Merchant", parseMerhantUDF($"merchant")).drop($"merchant")
      parsedTransaction.show(100,false)

    } catch {
      case e: Exception => println(e.printStackTrace())
    }

  }
}