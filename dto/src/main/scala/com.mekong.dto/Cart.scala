package com.mekong.dto

import java.time.{LocalDate, LocalDateTime}

case class Cart(userId: String, cartId: String, issuedTimestamp: LocalDateTime, deliveredOn: LocalDate,
                orders: List[Order])
