package com.mekong.dto
import org.joda.time._

case class Cart(cardId: Id[Cart], userId: String, issuedTimestamp: Instant, deliveredOn: Instant,
                orders: List[Order])
