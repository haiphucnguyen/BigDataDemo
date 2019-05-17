package com.mekong.dto

case class Order(orderId: Id[Order], productId: String, productCategory: String, price: Double, amount: Int)
