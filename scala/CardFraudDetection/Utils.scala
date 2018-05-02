package CardFraudDetection

object Utils {
  val parseDateTime=(date:String,time:String)=>{
    try {
      val finalDate=date.split(" +")(0)+" "+time
      finalDate
    }
    catch {
      case e:Exception=>println(e.printStackTrace())
      date
    }
  }
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