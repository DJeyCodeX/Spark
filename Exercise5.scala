import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._


object Exercise5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("/home/knoldus/Desktop/data.csv")

    df.show()


    val datetype = df.withColumn("StartDate", to_date(expr("sub_start_date")))
      .withColumn("EndDate", to_date(expr("sub_end_date")))
      .drop("sub_start_date", "sub_end_date")

    val datetyperegex = df
      .withColumn("Start_Date_only", regexp_replace(expr("sub_start_date"), "(\\d+)[:](\\d+)[:](\\d+).*$", ""))

    datetyperegex.show()

    val datalocationsplit = df.withColumn("City", split(expr("location"), "-")(0))
      .withColumn("State", split(expr("location"), "-")(1))
      .drop("location")

    val subscription = datalocationsplit.withColumn("SubscriptionLength",
      datediff(to_date(expr("sub_end_date")),to_date(expr("sub_start_date"))))

    subscription.show()

    val subscription1 = datalocationsplit.withColumn("SubscriptionLength",
      datediff(to_date(expr("sub_end_date")),to_date(expr("sub_start_date")))/30)

    subscription1.show()

    val subscription2 = datalocationsplit.withColumn("SubscriptionLength",
      round(datediff(to_date(expr("sub_end_date")),to_date(expr("sub_start_date")))/30,1))

    subscription2.show()

    val subscription3 = datalocationsplit.withColumn("SubscriptionLength",
      round(datediff(to_date(expr("sub_end_date")),to_date(expr("sub_start_date")))/30).cast("integer"))

    subscription3.show()

    val subscription4 = datalocationsplit.withColumn("SubscriptionLength",
      ceil(datediff(to_date(expr("sub_end_date")),to_date(expr("sub_start_date")))/30).cast("integer"))

    subscription4.show()

    val subscription5 = datalocationsplit.withColumn("ExtendedDate", date_add(to_date(expr("sub_start_date")), 90))

    subscription5.show()

    val subscription6 = datalocationsplit.withColumn("ExtendedDate", date_format(date_add(to_date(expr("sub_start_date")), 90), "dd-MMM-yyyy"))

    subscription6.show()










  }
}
