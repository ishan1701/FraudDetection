package CardFraudDetection

import org.apache.spark.SparkConf
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.log4j
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.datediff
import org.apache.hadoop.mapreduce.FileSystemCounter
import org.apache.hadoop.fs.FileSystem
import com.typesafe.config.ConfigFactory

object ParseTransactionData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Detection Model Builing")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val config = ConfigFactory.load("application.properties")
    try {
      val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ",")
      val transactionData = spark.read.options(readOption).csv(config.getString("transactionDataPath"))
      val customerData = spark.read.options(readOption).csv(config.getString("customerDataPath"))
        .select("cc_num", "lat", "long", "dob")
      val parseDateTimeUDF = udf(Utils.parseDateTime)
      val distanceUDF = udf(Utils.getDistance _)
      Utils.deleteOutputPath(config, fs)
      val parsedTransaction = transactionData.withColumn("transaction_time", parseDateTimeUDF($"trans_date", $"trans_time").cast("timestamp")).drop("trans_date", "trans_time")
      val processedTransactionDF = parsedTransaction.join(customerData, "cc_num").withColumn("distance", round(distanceUDF($"lat", $"long", $"merch_lat", $"merch_long"), 2)).drop("lat", "long", "merch_lat", "merch_long")
        .withColumn("age", round(datediff($"transaction_time", $"dob") / 365, 2)).withColumn("cardNum", $"cc_num".cast("string"))
        .select($"cardNum", $"age", $"category", $"merchant", $"distance", $"amt", $"is_fraud")

      val processedTransationIndexed = MLTransformaions.categoryIndexer(processedTransactionDF)
        .merchantIndexer().creditCardIndexer()
        .getParsedDataframe().persist()

      val processedTransationEncoded = MLTransformaions.oneHotEncoderCategory(processedTransationIndexed)
        .oneHotEncoderCreditCard()
        .oneHotEncoderMerchant()
        .getParsedOneHotEncoderDataframe()

      val (isImbalanced, labelToRduce, numOfClusters) = Utils.checkImbalancedCondition(processedTransationEncoded, spark)
      //print(s"dataset is $isImbalanced. Label to reeduce -> $labelToRduce. with clusrter= $numOfClusters")

      val transactionBalanced = {
        if (isImbalanced == true)
          MLTransformaions.banlancingDataSet(processedTransationEncoded, numOfClusters, spark)
        else
          processedTransationEncoded
      }
      val LRModel = MLTransformaions.logisticRegressionClassifier(transactionBalanced, spark)
      LRModel.save(config.getString("modelPath"))

    } catch {
      case e: Exception => println(e.printStackTrace())
    }

  }
}