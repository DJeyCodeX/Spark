import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.functions.expr


object Exercise4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.createDataFrame(Seq((1001, "Amit" , "amit@hadoopexam.com", "Mumbai",7000),
      (1002, "Rakesh" , "rakesh@hadoopexam.com",null,8000), (1003, "Rohit" , "rohit@hadoopexam.com", "Pune",9000),
      (1004, "Vinod" , "vinod@hadoopexam.com", null, 8000), (1005, "Venu" , "venu@hadoopexam.com", null, 6000),
      (1006, "Shyam" , "shyam@hadoopexam.com", "Newyork", 8000), (1007, "John" , "john@hadoopexam.com", null, 6000)))
      .toDF("user_id", "name", "email", "city", "fee")

    //data.filter(expr("city is null"))

    val df = data.filter(expr("city is null")).show()

    //Replace all null city with UNKNOWN
    val df1= data.na.fill("UNKNOWN").show()

    //get the total fee across all records
    val df2 = data.selectExpr("fee").groupBy().sum().show()
    println(df2)

    val df3 = data.selectExpr("fee").distinct().show()

    val df4 = data.selectExpr("fee").distinct().groupBy().sum().show()


  }
}
