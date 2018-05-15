package CardStreamingJob

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import CardFraudDetection.Utils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import CardFraudDetection.MLTransformaions
import org.apache.spark.sql.functions.hour
import org.apache.spark.ml.classification.{ LogisticRegressionModel, LogisticRegression }
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.joda.time.format.DateTimeFormat
import org.apache.spark.ml.{ PipelineModel, Pipeline }

object DetectTransactionClass {
  def addWhetherFraud(prediction: Double) = {
    if (prediction == 1.0)
      "Fraud"
    else
      "Not Fraud"
  }
  def parsedColumns(s: String) = {
    val parsedString: ArrayBuffer[String] = ArrayBuffer()
    var parsedStringFinal = ""
    if (s.contains("\"")) {
      val b = s.split(",").flatMap(x => x.split("'")).filter(x => x != "")
      for (i <- 0 to b.length - 1) {
        if (b(i).startsWith(" ")) {
          parsedString.remove(parsedString.length - 1)
          parsedString.append(b(i - 1) + "," + b(i))
        } else
          parsedString.append(b(i))
      }
      // parsedString.toString()
      for (i <- parsedString)
        parsedStringFinal += s"#$i"
      parsedStringFinal = parsedStringFinal.replaceFirst("#", "")

      parsedStringFinal
    } else {
      val b = s.replaceAll(",", "#")
      b
    }
  }
  def parseMerchantName(s: String) = {
    if (s.contains("\""))
      s.replace("\"", "")
    else
      s
  }
  def convertStrigtoDate(s: String) = {
    var date_string = ""
    val a = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val b = a.parseDateTime(s)
    b.getYear.toString() + "-" + (b.getMonthOfYear).toString() + "-" + (b.getDayOfMonth).toString()
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Fraud Detection")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val config = ConfigFactory.load("application.properties")
    import spark.implicits._
    val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true")
    val schema = StructType(
      Array(
        StructField("cc_num", LongType, true),
        StructField("first", StringType, true),
        StructField("last", StringType, true),
        StructField("trans_num", StringType, true),
        StructField("trans_date", StringType, true),
        StructField("trans_time", StringType, true),
        StructField("unix_time", StringType, true),
        StructField("category", StringType, true),
        StructField("merchant", StringType, true),
        StructField("amt", IntegerType, true),
        StructField("merch_lat", DoubleType, true),
        StructField("merch_long", DoubleType, true)))

    val transaction = spark.sparkContext.textFile("file:\\G:\\Ishan\\MachineLeaning\\FraudDetection\\modelTesting\\streamedData.txt")
      .map(x => parsedColumns(x)).map(x => x.split("#").filter(x => x != ""))
      .map(x => Row(x(0).toLong, x(1), x(2), x(3), convertStrigtoDate(x(4)), x(5), x(6), x(7), x(8), x(9).toInt, x(10).toDouble, x(11).toDouble)).cache()
    transaction.first()
    //transaction.collect().foreach(println)
    val transactionData = spark.createDataFrame(transaction, schema).drop($"first").drop($"last")

    val customerData = spark.read.options(readOption).csv(config.getString("customerDataPath"))
    // .select("cc_num", "lat", "long", "dob")

    //val transactionData = spark.read.options(readOption).csv("file:\\G:\\Ishan\\MachineLeaning\\FraudDetection\\modelTesting\\streamedData.csv")
    // val transactionData = spark.read.options(readOption).csv("G:\\Ishan\\MachineLeaning\\FraudDetection\\modelTesting\\streamedData.csv").drop($"first").drop($"last")
    transactionData.printSchema()
    val parseDateTimeUDF = udf(Utils.parseDateTime)
    val distanceUDF = udf(Utils.getDistance _)
    val parseMerchantNameUDF = udf(parseMerchantName _)
    val parsedTransaction = transactionData.withColumn("transaction_time", parseDateTimeUDF($"trans_date", $"trans_time").cast("timestamp")).drop("trans_date", "trans_time")
    parsedTransaction.printSchema()
    parsedTransaction.show()
    val processedTransactionDF = parsedTransaction.join(customerData, "cc_num").withColumn("distance", round(distanceUDF($"lat", $"long", $"merch_lat", $"merch_long"), 2)).drop("lat", "long", "merch_lat", "merch_long")
      .withColumn("age", round(datediff($"transaction_time", $"dob") / 365, 2)).withColumn("cardNum", $"cc_num".cast("string")).withColumn("merchant", parseMerchantNameUDF($"merchant"))
      .select($"cardNum", $"trans_num", $"transaction_time", $"first", $"last", $"age", $"category", $"merchant", $"distance", $"amt")
    processedTransactionDF.show()
    val pipeLineModel = PipelineModel.load(config.getString("pipeLineModelPath"))
    val processedTransationEncoded = pipeLineModel.transform(processedTransactionDF)
    processedTransationEncoded.show(5, false)
    val processedTransationVector = MLTransformaions.vectorAssembler(processedTransationEncoded)
    processedTransationVector.show(false)
    val model = RandomForestClassificationModel.load(config.getString("modelPath"))
    println(model.numFeatures)

    val predictionDF = model.transform(processedTransationVector)
    val addFraudCol = udf(addWhetherFraud _)
    val prediction = predictionDF.select($"cardNum", $"trans_num", $"transaction_time", $"first", $"last", $"age", $"category", $"merchant", $"amt", $"prediction").withColumn("Fraud OR Non-Fraud", addFraudCol($"prediction")).drop($"prediction")

    prediction.show(false)
    // println(prediction.filter($"label"===$"prediction").count())

  }
}