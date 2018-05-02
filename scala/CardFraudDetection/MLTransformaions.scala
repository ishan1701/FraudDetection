package CardFraudDetection

import org.apache.spark.ml.feature.StringIndexer

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
    val indexer = new StringIndexer().setInputCol("cc_num").setOutputCol("cc_numIndexex")
    indexedDF = indexer.fit(df).transform(df)
    this
  }
  def getParsedDataframe() = {
    indexedDF
  }
}