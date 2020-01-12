import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.functions._


object TopNDailyProducts_2_itversity {
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

    /*       orders.createTempView("orders")
        orderItems.createTempView("order_items")*/

    spark.conf.set("spark.sql.shuffle.partitions", "2")

    //finding the TopNDailyProducts

    //1. Create Window Spec object using partitionBy to get aggregations with in each group

    //val spec = Window.partitionBy(orderItems("order_item_order_id"))

    //orderItems.withColumn("order_revenue", sum(orderItems("order_item_subtotal")).over(spec)).withColumn("order_avg_revenue", avg(orderItems("order_item_subtotal")).over(spec)).show(10)

    //2. Get average salary foreach department and get all employee details who earn more than averagesalary

    //3. get average revenue for each day and get all the orders who earn revenue more than average revenue

    //4. get highest order revenue and get all the orders which have revenue more than 75% of the revenue

    //diff b/w order item which generated highest revenue and lowest revenue

    //val spec = Window.partitionBy(orderItems("order_item_order_id")).orderBy(orderItems("order_item_subtotal").desc)
    //orderItems.withColumn("next_order_item_revenue", lead("order_item_subtotal", 1).over(spec)).show(10)
    //  val leadOrderItems = orderItems.withColumn("next_order_item_revenue", lead("order_item_subtotal", 1).over(spec)).orderBy(orderItems("order_item_order_id"),orderItems("order_item_subtotal").desc).drop("order_item_product_id", "order_item_quantity", "order_item_product_price")
    //leadOrderItems.withColumn("revenue_diff", leadOrderItems("order_item_subtotal") - leadOrderItems("next_order_item_revenue")).show(10)
    // rank and dense rank

    //let say we have students who got marks as S1-> 100, s2-> 99, s3-> 98 s4-> 98 s5 -> 98 s6-> 97
    // so rank function will rank as 1,2,3,3,3,6 it will skip 4,5
    //but in case of dense rank  it will give 1,2,3,3,3,4 it won't skip 4,5
    //in case of row_numbers it will be 1,2,3,4,5,6   --> it wont repeat 3 but will get 3,4,5

    val dailyProductRevenue = orders.where("""order_status in ("COMPLETE","CLOSED")""")
      .join(orderItems, orders("order_id") === orderItems("order_item_order_id"))
      .groupBy(orders("order_date"), orderItems("order_item_product_id"))
      .agg(round(sum(orderItems("order_item_subtotal")), 2).alias("revenue"))

    val spec = Window.partitionBy(dailyProductRevenue("order_date")).orderBy(dailyProductRevenue("revenue").desc)

    val dailyProductRevenueRanked = dailyProductRevenue.withColumn("rnk", rank().over(spec))

    val topN = sys.env("value")

    //actual code
    //dailyProductRevenueRanked.orderBy(dailyProductRevenueRanked("order_date"), dailyProductRevenueRanked("revenue").desc).show()

    //actual code with filter
    dailyProductRevenueRanked.where(s"rnk <= $topN")
      .orderBy(dailyProductRevenueRanked("order_date"), dailyProductRevenueRanked("revenue").desc).drop("rnk").show()

  }
}
