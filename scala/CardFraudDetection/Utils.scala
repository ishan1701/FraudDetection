package CardFraudDetection

object Utils {
  // For sql qyeries
  /*def parseMerchant(merchant: String):String= {
    try {
      if (merchant.contains("fraud_")) {
        val splitParts = merchant.split("_")
        splitParts(0)
      }
      else
        merchant
    }
    catch {
      case e:Exception=>println(e.printStackTrace())
      merchant
    }
  }*/
  
  // for DataFrame API
  val parseMerchnat = (merchant: String) => {
    try {
      if (merchant.contains("fraud_")) {
        val splitParts = merchant.split("_")
        splitParts(1)
      } else
        merchant
    } catch {
      case e: Exception =>
        println(e.printStackTrace())
        merchant
    }
  }
}