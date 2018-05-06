package CardFraudDetection

case class Transaction(cc_numIndexed: Double, categoryIndexed: Double, merchantIndexed: Double, merch_lat: Double, merch_long: Double, amt: Double, is_fraud: Int)