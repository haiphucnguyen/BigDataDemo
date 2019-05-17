package com.mekong.dto

import java.time.Instant

case class ShippingStatus(orderId: Id[Order], status: String, changeDate: Instant)
