package com.mekong.dto

import java.time.LocalDateTime

case class ShippingStatus(orderId:String, status:String, changeDate: LocalDateTime)
