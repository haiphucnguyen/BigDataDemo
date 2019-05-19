package com.mekong.dto

import java.time.Instant

case class Cart(cardId: String, userId: String, issuedTimestamp: Instant, deliveredOn: Instant,
                orders: List[Order])
