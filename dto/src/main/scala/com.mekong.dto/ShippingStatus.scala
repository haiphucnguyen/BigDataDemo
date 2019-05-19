package com.mekong.dto

import java.time.Instant

case class ShippingStatus(orderId: String, status: String, changeDate: Instant)
