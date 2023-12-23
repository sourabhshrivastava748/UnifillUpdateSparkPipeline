package entity

case class UniwareShippingPackage(
     mobile: String,
     tenant_code: String,
     facility_code: String,
     shipping_package_code: String,
     shipping_provider_source_code: String,
     shipping_courier: String,
     payment_method: String,
     gmv: BigDecimal,
     quantity: java.lang.Long
)
