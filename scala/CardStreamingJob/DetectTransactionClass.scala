package CardStreamingJob

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import CardFraudDetection.Utils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import CardFraudDetection.MLTransformaions
import org.apache.spark.sql.functions.hour
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.LogisticRegression
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier

object DetectTransactionClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Fraud Detection")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val config = ConfigFactory.load("application.properties")
    import spark.implicits._
    val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true")
    val transactionData = spark.read.options(readOption).csv(config.getString("transactionDataPath"))
    val customerData = spark.read.options(readOption).csv(config.getString("customerDataPath"))
      .select("cc_num", "lat", "long", "dob")
    val parseDateTimeUDF = udf(Utils.parseDateTime)
    val distanceUDF = udf(Utils.getDistance _)
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
      .getParsedOneHotEncoderDataframe().cache()
      processedTransationEncoded.show(2,false)
    val processedTransationVector=MLTransformaions.vectorAssembler(processedTransationEncoded)
    processedTransationVector.show(2,false)
    val model=RandomForestClassificationModel.load(config.getString("modelPath"))
    val prediction=model.transform(processedTransationVector).select($"label", $"prediction")
    println(prediction.count())
    println(prediction.filter($"label"===$"prediction").count())
    
  }
}