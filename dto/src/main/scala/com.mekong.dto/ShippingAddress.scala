package com.mekong.dto

case class ShippingAddress(cartId: Id[Cart], address: String, city: String, zipCode: String, state: String)
