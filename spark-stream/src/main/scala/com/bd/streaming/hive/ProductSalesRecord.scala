package com.bd.streaming.hive

case class ProductSalesRecord(productId: String, productCategory: String, totalAmount: Double, issueTimeStamp: Long)