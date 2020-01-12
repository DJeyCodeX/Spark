import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.functions._


object WindowFunctions_1_itversity {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Windows Function").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ordersCSV = spark.read.csv("/home/knoldus/Desktop/part-00000").toDF("order_id", "order_date", "order_customer_id", "order_status")

    //ordersCSV.printSchema()
    //ordersCSV.show(10)

    val orderItemsCSV = spark.read.csv("file:///home/knoldus/Desktop/order_items_part-00000").toDF("order_item_id", "order_item_order_id", "order_item_product_id",
      "order_item_quantity", "order_item_subtotal", "order_item_product_price")

    //orderItemsCSV.printSchema()
    //orderItemsCSV.show(10)

    val orders = ordersCSV.withColumn("order_id", col("order_id").cast(IntegerType)).withColumn("order_customer_id", col("order_customer_id").cast(IntegerType))

    val orderItems = orderItemsCSV.withColumn("order_item_id", col("order_item_id").cast(IntegerType))
      .withColumn("order_item_order_id", col("order_item_order_id").cast(IntegerType))
      .withColumn("order_item_product_id", col("order_item_product_id").cast(IntegerType))
      .withColumn("order_item_quantity", col("order_item_quantity").cast(IntegerType))
      .withColumn("order_item_subtotal", col("order_item_subtotal").cast(FloatType))
      .withColumn("order_item_product_price", col("order_item_product_price").cast(FloatType))

/*    orders.createTempView("orders")
    orderItems.createTempView("order_items")*/

    spark.conf.set("spark.sql.shuffle.partitions", "2")

    /*val dailyProductRevenue = spark.sql(s"""select o.order_date, oi.order_item_product_id, round(sum(oi.order_item_subtotal),2) as revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id where o.order_status in ("COMPLETE", "CLOSED") group by o.order_date, oi.order_item_product_id order by o.order_date, revenue desc""")
    dailyProductRevenue.write.csv("/home/knoldus/Desktop/test")*/

    //val dailyProductRevenue = orders.where("""order_status in ("COMPLETE","CLOSED")""").join(orderItems, (col("orderItems.order_id") === col("orderItems.order_item_order_id")), "inner").groupBy(col("orders.order_date"), col("orderItems.order_item_product_id")).agg(round(sum(col("orderItems.order_item_subtotal")), 2).alias("revenue"))
    // 1.revenue for each order_item_order_id

//val revenuePerOrder = orderItems.groupBy(col("order_item_order_id")).agg(round(sum(col("order_item_subtotal")),2).alias("order_revenue"))


    //orderItems.join(revenuePerOrder, (orderItems("order_item_order_id") === revenuePerOrder("order_item_order_id"))).select(orderItems("order_item_order_id"),orderItems("order_item_subtotal"), revenuePerOrder("order_revenue")).show()


    //val dailyProductRevenueSorted = dailyProductRevenue.orderBy(col("dailyProductRevenueSorted.order_date"), col("dailyProductRevenue.revenue").desc)
    //dailyProductRevenueSorted.write.csv("/home/knoldus/Desktop/test1")

//finding the orderrevenue using Window partition by method instead of using join
val spec = Window.partitionBy(orderItems("order_item_order_id"))

    orderItems.select(orderItems("order_item_order_id"),orderItems("order_item_subtotal"), sum(orderItems("order_item_subtotal")).over(spec).alias("order_revenue")).show()
  }
}
