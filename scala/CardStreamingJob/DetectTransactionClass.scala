/*package CardStreamingJob

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import CardFraudDetection.Utils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import CardFraudDetection.MLTransformaions
import org.apache.spark.sql.functions.hour
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.LogisticRegression

object DetectTransactionClass {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Fraud Detection")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ",")
    val readPath = "G:\\Ishan\\MachineLeaning\\Pramod Data\\Data\\FinalCardData\\transactions.csv"
    val transactionData = spark.read.options(readOption).csv(readPath)
   val parseMerhantUDF = udf(Utils.parseMerchnat)
    val parseDateTimeUDF = udf(Utils.parseDateTime)
    val parsedTransaction = transactionData.withColumn("Merchant", parseMerhantUDF($"merchant"))
      .drop($"merchant").withColumn("transaction_time", parseDateTimeUDF($"trans_date", $"trans_time").cast("timestamp")).drop($"trans_date").drop($"trans_time")
      .withColumn("hourOFSwipe", hour($"transaction_time"))
    val parsedTransationIndexed = MLTransformaions.categoryIndexer(parsedTransaction)
      .merchantIndexer().creditCardIndexer()
      .getParsedDataframe().persist()
    val parsedTransationIndexedEncoded = MLTransformaions.oneHotEncoderCategory(parsedTransationIndexed)
      .oneHotEncoderCreditCard()
      .oneHotEncoderMerchant()
      .getParsedOneHotEncoderDataframe()
      parsedTransationIndexedEncoded.printSchema()
     val parsedTrasactionVectorIndexed=MLTransformaions.vectorAssembler(parsedTransationIndexedEncoded)
     parsedTrasactionVectorIndexed.printSchema()
     parsedTrasactionVectorIndexed.show(2)
    //val parsedTransactionAssemled=MLTransformaions.vectorAssembler(parsedTransationIndexedEncoded)
    //val vectorAssembler=new VectorAssembler().setInputCols(value)
   // parsedTransactionAssemled.show()
      val LRModel=LogisticRegressionModel.load("G:\\Ishan\\MachineLeaning\\weka\\outMoel")
      val a=LRModel.transform(parsedTrasactionVectorIndexed)
      println(a.count())
     a.filter($"prediction"===1).show(200,false)
  }
}*/