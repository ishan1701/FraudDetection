package CardFraudDetection

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.clustering.{ KMeans, KMeansModel }
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.classification.{LogisticRegressionModel,LogisticRegression}

object MLTransformaions {
  private var indexedDF: org.apache.spark.sql.DataFrame = _
  def categoryIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def merchantIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("Merchant").setOutputCol("merchantIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def creditCardIndexer(df: org.apache.spark.sql.DataFrame = indexedDF) = {
    val indexer = new StringIndexer().setInputCol("cc_num").setOutputCol("cc_numIndexed")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def vectorAssembler(df: org.apache.spark.sql.DataFrame) = {
    val columnNames = Array("cc_numIndexed", "categoryIndexed", "merchantIndexed", "merch_lat", "merch_long", "amt")
    val assembler = new VectorAssembler().setInputCols(columnNames).setOutputCol("features")
    assembler.transform(df).select("is_fraud", "features").withColumnRenamed("is_fraud", "label")
  }
  def getParsedDataframe() = {
    indexedDF
  }
  def vectorAssemblerForSampling(df: org.apache.spark.sql.DataFrame) = {
    val coloumnNames = Array("cc_numIndexed", "categoryIndexed", "merchantIndexed", "merch_lat", "merch_long", "amt")
    val assembler = new VectorAssembler().setInputCols(coloumnNames).setOutputCol("features")
    assembler.transform(df)

  }
  def banlancingDataSet(df: org.apache.spark.sql.DataFrame, numOfClusters: Int, spark: org.apache.spark.sql.SparkSession) = {
    import spark.implicits._
    val filteredDFWithMoreLables = df.filter($"is_fraud" === 0)
    val filteredDfWithLessLabels = df.filter($"is_fraud" === 1)
    val TransactionWithFeatures = vectorAssemblerForSampling(filteredDFWithMoreLables)
    val kMeans = new KMeans().setK(numOfClusters).setMaxIter(30)
    TransactionWithFeatures.show()
    val data = df.cache()
    val model = kMeans.fit(TransactionWithFeatures)
    val clusterCentres = model.clusterCenters
    //    for(i<-clusterCentres)
    //      println(i)
    val transactionDFAfterSampling = spark.sparkContext.parallelize(clusterCentres).map(x => (x, 0)).map(x => Utils.vectorToRDD(x)).toDF()
    vectorAssembler(filteredDfWithLessLabels).unionAll(vectorAssembler(transactionDFAfterSampling))

  }
  /*def randomForestClassifier(df: org.apache.spark.sql.DataFrame) = {
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val randomForestEstimator = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(700)
    val model = randomForestEstimator.fit(training)
    val transactionwithPredic = model.transform(test)
    println(transactionwithPredic.count())
    transactionwithPredic.show(300)

  }*/
  def logisticRegressionClassifier(df:org.apache.spark.sql.DataFrame)={
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))
    val logisticEstimator=new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
    val model=logisticEstimator.fit(training)
    val transactionwithPredic = model.transform(test)
   println(transactionwithPredic.count())
//    transactionwithPredic.show(300)
    transactionwithPredic
    
  }
  }
  