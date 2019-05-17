package com.mekong.dto

import java.time.Instant

case class Cart(cardId: Id[Cart], userId: String, issuedTimestamp: Instant, deliveredOn: Instant,
                orders: List[Order])
