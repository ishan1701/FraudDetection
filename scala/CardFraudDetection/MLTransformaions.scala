package CardFraudDetection

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
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
    val columnNames = Array("cc_numIndexed", "categoryIndexed", "merchantIndexed", "merch_lat", "merch_long", "hourOFSwipe","amt")
    val assembler = new VectorAssembler().setInputCols(columnNames).setOutputCol("features")
    assembler.transform(df).select("is_fraud", "features").withColumnRenamed("is_fraud", "label")
  }
  def getParsedDataframe() = {
    indexedDF
  }
}