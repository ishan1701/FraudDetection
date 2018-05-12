package CardFraudDetection

//", "", "", "merch_lat", "merch_long", ""
case class Transaction(cc_numIndexedOneHot: Double, categoryIndexedOneHot: Double, merchantIndexedOneHot: Double, merch_lat: Double, merch_long: Double, amt: Double, is_fraud: Int)