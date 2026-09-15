package com.databricks.migrationgenie.scalamigrationexample.orderprocessing

/** Domain model for the order-processing pipeline.
  *
  * These case classes back the RDDs used throughout Bronze/Silver and the DataFrames
  * used for the Gold joins. Fields, primary keys, and foreign keys follow spec §3:
  *
  *   region       PK regionId
  *   product      PK productId
  *   order        PK orderId          FK regionId -> region
  *   orderDetail  PK orderDetailId    FK orderId -> order, productId -> product
  */
object Models {

  final case class Region(regionId: Int, regionName: String)

  final case class Product(productId: Int, productName: String, category: String, unitPrice: Double)

  final case class Order(orderId: Int, orderTs: String, regionId: Int, customerId: Int, status: String)

  final case class OrderDetail(orderDetailId: Int, orderId: Int, productId: Int, quantity: Int, unitPrice: Double)
}
