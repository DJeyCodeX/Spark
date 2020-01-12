import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise39 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")


        //find the previous day date
        spark.sql("select date_sub(current_timestamp(), 1)").show(false)

        //find current date
        spark.sql("select current_date").show(false)

        //find the date after 2 days from today
        spark.sql("select date_add(current_timestamp(), 2)").show(false)

        //find the date 2 daysfore be from today
        spark.sql("select date_sub(current_timestamp(), 2)").show(false)

        //find the date after two months from today
        spark.sql("select add_months(current_timestamp(), 2)").show(false)

        //diff b/w two dates (current date - current date + 2 months)
        //find the number of days between todays date and same date after two month)
        spark.sql("select datediff(current_date(), add_months(current_date(), 2))").show(false)

        //getting the last date of the month
        //how to find the lst day date in current month

        spark.sql("select last_day(current_date())").show(false)

        //how you can find the hour portion from current timestamp

        spark.sql("select hour(current_timestamp())").show(false)

        //how you can find the month portion from current imestamp
        spark.sql("select month(current_timestamp())").show(false)

        //getting number of months between 2 dates
        spark.sql("select months_between(add_months(current_date(), 2), current_date())").show(false)

        //get the querter of the date
        //how would you find that todays date falls under which querter
        spark.sql("select quarter(current_timestamp())").show(false)



        //GETTING SIMILAR OUTPUT FROM DATSETS API

        spark.range(1).select(current_timestamp()).show()
        spark.range(1).select(hour(current_timestamp())).show()
        spark.range(1).select(last_day(current_timestamp())).show()

        //format current timestamp in date formats

        spark.range(1).select(date_format(current_timestamp(), "dd-MMM-yyyy")).show()
        spark.range(1).select(date_format(current_timestamp(), "dd-MMM-yyyy hh:mm:ss")).show()

        //how would you get unix epoc timestamp
        spark.range(1).select(unix_timestamp()).show()

        //how would you convert this string "28-07-2018" in date format
        spark.range(1).select(to_date(lit("2018-07-27"))).show()
        //create window with slide duration
        //create a window from current timestamp for every 5 minutes with sliding interval 1 minute

        spark.sql("select window(current_timestamp(), '5 minutes', '1 minutes')").show(10, false)

        val hecourses = spark.sparkContext.parallelize(Seq(

                                                              (1, "2018-01-01", 20000),
                                                              (1, "2018-01-02", 23000),
                                                              (1, "2018-01-03", 90000),
                                                              (1, "2018-01-04", 55000),
                                                              (1, "2018-01-05", 20000),
                                                              (1, "2018-01-06", 23000),
                                                              (1, "2018-01-07", 90000),
                                                              (1, "2018-01-08", 55000),
                                                              (2, "2018-01-01", 80000),
                                                              (2, "2018-01-02", 90000),
                                                              (2, "2018-01-03", 100000),
                                                              (2, "2018-01-04", 80000),
                                                              (2, "2018-01-05", 90000),
                                                              (2, "2018-01-06", 100000),
                                                              (2, "2018-01-07", 80000),
                                                              (2, "2018-01-08", 90000)
                                                              )).toDF("course_id", "startDate", "fee")
                .withColumn("startDate", col("startDate").cast("date"))

        hecourses.show()











    }
}
