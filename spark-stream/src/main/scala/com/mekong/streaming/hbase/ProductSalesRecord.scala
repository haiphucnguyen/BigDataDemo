package com.mekong.streaming.hbase

case class ProductSalesRecord(itemKey: String, productId: String, productCategory: String, totalAmount: Double, issueTimeStamp: Long)