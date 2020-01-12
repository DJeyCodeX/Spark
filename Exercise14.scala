import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise14 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        val schema = StructType(Array(StructField("id", IntegerType, false),
                                      StructField("name", StringType, false),
                                      StructField("fee", DoubleType, false),
                                      StructField("venue", StringType, false),
                                      StructField("Duration", IntegerType, false),
                                      StructField("SubStartDate", StringType, false),
                                      StructField("SubEndDate", StringType, false)))


        val Row1 = Row(1001, "Amit Kumar", 10000.0, "Mumbai", 5, "28-Jan-2018", "28-Jan-2019")
        val Row2 = Row(1002, "John", 10000.0, "Mumbai", 5, "21-Feb-2018", "21-Feb-2019")
        val Row3 = Row(1003, "Venkat", 10000.0, "Delhi", 5, "11-Mar-2018", "11-Jun-2019")
        val Row4 = Row(1004, "Sarfraj", 10000.0, "Kolkata", 5, "28-Apr-2018", "28-Aug-2019")
        val Row5 = Row(1005, "Manoj", 11000.0, "Banglore", 5, "28-Jan-2019", "28-Jan-2020")
        val Row6 = Row(1006, "Jasmin", 11000.0, "Mumbai", 5, "28-Jan-2018", "28-Jan-2021")
        val Row7 = Row(1007, "Reegal", 11000.0, "Banglore", 5, "28-Jan-2018", "28-Jan-2020")
        val Row8 = Row(1008, "Sayed", 11000.0, "Banglore", 5, "28-Jan-2018", "28-Jan-2021")
        val Row9 = Row(1009, "Mike", 15000.0, "Newyork", 7, "28-Jan-2018", "28-Jan-2020")
        val Row10 = Row(1010, "Javier", 14000.0, "Washington", 3, "28-Jan-2018", "28-Jan-2022")
        val Row11 = Row(1011, "Ronak", 16000.0, "London", 4, "28-Jan-2018", "28-Jan-2024")
        val Row12 = Row(1012, "Fiaz", 19000.0, "Baltimor", 7, "28-Jan-2018", "28-Jan-2023")
        val Row13 = Row(1013, "Vikram", 19000.0, "Baltimor", 7, "28-Jan-2018", "28-Jan-2021")
        val Row14 = Row(1014, "Deepak", 19000.0, "Baltimor", 7, "28-Jan-2018", "28-Jan-2020")
        val Row15 = Row(1015, "Venugopal", 19000.0, "Baltimor", 7, "28-Jan-2018", "28-May-2019")


        val data = Seq(Row1, Row2, Row3, Row4, Row5, Row6, Row7, Row8, Row9, Row10, Row11, Row12, Row13, Row14, Row15)

        val datadf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        val df = datadf.withColumn("StartDate", to_date(expr("SubStartDate"), "dd-MMM-yyyy"))
                .withColumn("EndDate", to_date(expr("SubEndDate"), "dd-MMM-yyyy")).drop("SubStartDate").drop("SubEndDate")

        val df1 = df.withColumn("MonthSub", months_between(expr("EndDate"), expr("StartDate"))).show()

        val df2 = df.withColumn("yearSub", months_between(expr("EndDate"), expr("StartDate"))/12).show()

        val df3 = df.withColumn("DaysSub", datediff(expr("EndDate"), expr("StartDate"))).show()

        val df4 = df.withColumn("PendingDays", datediff(expr("EndDate"), current_date()))

        val df5 = df4.withColumn("SubStatus", when(expr("PendingDays < 0"), lit("Expired")).otherwise(lit("Active")))
                .show()

        val df6 = df.withColumn("StartDateInd", date_format(expr("StartDate"), "dd-MMM-yyyy"))
                .withColumn("EndDateInd", date_format(expr("EndDate"), "dd-MMM-yyyy")).show()

    }
}
