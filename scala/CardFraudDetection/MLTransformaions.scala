package CardFraudDetection

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.clustering.{ KMeans, KMeansModel }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.classification.{ LogisticRegressionModel, LogisticRegression }
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.{ Vector, Vectors }

case class trasactionDFKMeans(label: Int, features: Vector)

object MLTransformaions {
  /*private var indexedDF: org.apache.spark.sql.DataFrame = _
  private var onehotIndexerDF: org.apache.spark.sql.DataFrame = _

  def categoryIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def merchantIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("merchant").setOutputCol("merchantIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def creditCardIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("cardNum").setOutputCol("cardnumIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }

  def getParsedDataframe() = {
    indexedDF
  }
  def oneHotEncoderCreditCard(df: org.apache.spark.sql.DataFrame = onehotIndexerDF) = {
    val oneHotEncoder = new OneHotEncoder().setInputCol("cardnumIndexed").setOutputCol("cardnumIndexeddOneHot")
    onehotIndexerDF = oneHotEncoder.transform(df)
    this

  }
  def oneHotEncoderCategory(df: org.apache.spark.sql.DataFrame = onehotIndexerDF) = {
    val oneHotEncoder = new OneHotEncoder().setInputCol("categoryIndexed").setOutputCol("categoryIndexedOneHot")
    onehotIndexerDF = oneHotEncoder.transform(df)
    this

  }
  def oneHotEncoderMerchant(df: org.apache.spark.sql.DataFrame = onehotIndexerDF) = {
    val oneHotEncoder = new OneHotEncoder().setInputCol("merchantIndexed").setOutputCol("merchantIndexedOneHot")
    onehotIndexerDF = oneHotEncoder.transform(df)
    this
  }

  def getParsedOneHotEncoderDataframe() = {
    onehotIndexerDF
  }

*/  
  val categoryIndexer=new StringIndexer().setInputCol("category").setOutputCol("categoryIndexed")
  val merchantIndexer=new StringIndexer().setInputCol("merchant").setOutputCol("merchantIndexed")
  val creditCardIndexer=new  StringIndexer().setInputCol("cardNum").setOutputCol("cardnumIndexed")
  val oneHotEncoderCreditCard = new OneHotEncoder().setInputCol("cardnumIndexed").setOutputCol("cardnumIndexeddOneHot")
  val oneHotEncoderCategory = new OneHotEncoder().setInputCol("categoryIndexed").setOutputCol("categoryIndexedOneHot")
  val oneHotEncoderMerchant = new OneHotEncoder().setInputCol("merchantIndexed").setOutputCol("merchantIndexedOneHot")
  
  
  def vectorAssemblerForSampling(df: org.apache.spark.sql.DataFrame) = {
    val coloumnNames = Array("cardnumIndexeddOneHot", "categoryIndexedOneHot", "merchantIndexedOneHot", "age", "distance", "amt")
    val assembler = new VectorAssembler().setInputCols(coloumnNames).setOutputCol("features")
    assembler.transform(df)

  }
  def vectorAssembler(df: org.apache.spark.sql.DataFrame) = {
    val columnNames = Array("cardnumIndexeddOneHot", "categoryIndexedOneHot", "merchantIndexedOneHot", "age", "distance", "amt")
    val assembler = new VectorAssembler().setInputCols(columnNames).setOutputCol("features")
    assembler.transform(df).withColumnRenamed("is_fraud", "label")

  }
  def banlancingDataSet(df: org.apache.spark.sql.DataFrame, numOfClusters: Int, spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    val filteredDFWithMoreLables = df.filter($"is_fraud" === 0)
    val filteredDfWithLessLabels = df.filter($"is_fraud" === 1)
    val TransactionWithFeatures = vectorAssemblerForSampling(filteredDFWithMoreLables).cache()
    val kMeans = new KMeans().setK(numOfClusters).setMaxIter(30)
    //TransactionWithFeatures.show()
    val data = df.cache()
    val model = kMeans.fit(TransactionWithFeatures)
    val clusterCentres = model.clusterCenters
    val transactionDFAfterSampling = spark.sparkContext.parallelize(clusterCentres).map(x => x.toSparse).map(x => (0, x))
      .map(x => trasactionDFKMeans(x._1, x._2)).toDF()
    vectorAssembler(filteredDfWithLessLabels).select($"label", $"features").unionAll(transactionDFAfterSampling)

  }
  def randomForestClassifier(df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionwithPrediction = model.transform(test)
    println(s"total data count is" + transactionwithPrediction.count())
    println("count of same label " + transactionwithPrediction.filter($"prediction" === $"label").count())
    model
  }

  /* def logisticRegressionClassifier(df: org.apache.spark.sql.DataFrame, spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    df.cache()
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val logisticEstimator = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
    val model = logisticEstimator.fit(training)
    val transactionwithPrediction = model.transform(test)
    println(s"total data count is"+transactionwithPrediction.count())
    println("count of same label "+transactionwithPrediction.filter($"prediction" === $"label").count())
    model
  }*/

}
  