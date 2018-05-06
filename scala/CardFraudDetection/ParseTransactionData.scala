package CardFraudDetection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.log4j
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.hour
import org.apache.hadoop.mapreduce.FileSystemCounter
import org.apache.hadoop.fs.FileSystem

object ParseTransactionData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CreditCard Detection")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // val logger = Logger.getLogger(ParseTransactionData.getClass)
    // System.setProperty("HADOOP.HOME.DIR", "G:\\Ishan\\hadoop")
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //Logger.getLogger("org").setLevel(Level.OFF)
    import spark.implicits._
    println(spark.version)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    try {
      val readOption: Map[String, String] = Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ",")
      val readPath = "G:\\Ishan\\MachineLeaning\\Pramod Data\\Data\\FinalCardData\\transactions.csv"
      //cc_num,first,last,trans_num,trans_date,trans_time,unix_time,category,merchant,amt,merch_lat,merch_long
      val transactionData = spark.read.options(readOption).csv(readPath)
      transactionData.show()
      val parseMerhantUDF = udf(Utils.parseMerchnat)
      val parseDateTimeUDF = udf(Utils.parseDateTime)
      val parsedTransaction = transactionData.withColumn("Merchant", parseMerhantUDF($"merchant"))
        .drop($"merchant").withColumn("transaction_time", parseDateTimeUDF($"trans_date", $"trans_time").cast("timestamp")).drop($"trans_date").drop($"trans_time")
        .withColumn("hourOFSwipe", hour($"transaction_time"))
      //parsedTransaction.show(false)
      // parsedTransaction.printSchema()
      val parsedTransationIndexed = MLTransformaions.categoryIndexer(parsedTransaction)
        .merchantIndexer().creditCardIndexer()
        .getParsedDataframe().persist()
      parsedTransationIndexed.printSchema()
      parsedTransationIndexed.show(false)
      // var parsedFeaturesTransactionBalanced = parsedTransationIndexed
      val (isImbalanced, labelToRduce, numOfClusters) = Utils.checkImbalancedCondition(parsedTransationIndexed, spark)
      print(s"dataset is $isImbalanced. Label to reeduce -> $labelToRduce. with clusrter= $numOfClusters")
      val balancedTrasactionDFIndexed = MLTransformaions.banlancingDataSet(parsedTransationIndexed, numOfClusters, spark).toDF()
      val transactionWithPrediction=MLTransformaions.logisticRegressionClassifier(balancedTrasactionDFIndexed).filter($"prediction"===$"label")
         println(transactionWithPrediction.count())
      //balancedTrasactionDFIndexed.show()
      //if(isImbalanced==true)

      // Utils.checkImbalancedCondition(parsedTransationIndexed, spark)

      //      i (Utils.checkImbalancedCondition(parsedTransationIndexed, spark)) {
      //         val parsedFeaturesTransactionVectorAssembled = MLTransformaions.vectorAssembler(parsedTransationIndexed)
      //
      //      }
      //

      /* else
        val parsedFeaturesTransaction = MLTransformaions.vectorAssembler(parsedTransationIndexed)
      */ //parsedTransationIndexed.coalesce(1).rdd.saveAsTextFile("G:/Ishan/MachineLeaning/Pramod Data/Data/FinalCardData/opt")//.write.csv("G:\\Ishan\\MachineLeaning\\Pramod Data\\Data\\FinalCardData\\Output")
      // val parsedFeaturesTransaction = MLTransformaions.vectorAssembler(parsedTransationIndexed).coalesce(1).write.csv("G:\\Ishan\\MachineLeaning\\Pramod Data\\Data\\FinalCardData\\Output")

    } catch {
      case e: Exception => println(e.printStackTrace())
    }

  }
}