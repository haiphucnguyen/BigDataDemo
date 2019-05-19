package repositories

object ShippingDB {
  import com.mekong.dto._
  def randomShipping(cart:Cart): ShippingAddress ={
    val address = ZipCodeDB.nextRandomAddress()
    new ShippingAddress(
        cart.cardId,
        address("address"),
        "city",
        address("zip"),
        address("state")
      )
  }
}
