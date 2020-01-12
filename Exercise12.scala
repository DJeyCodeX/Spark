import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise12 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    val schema = StructType(Array(StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("fee", DoubleType, false),
      StructField("venue", StringType, false),
      StructField("duration", IntegerType, false)))

    val Row1 = Row(1001, "Amit Kumar", 10000.0, "Mumbai", 5)
    val Row2 = Row(1002, "John", 10000.0, "Mumbai", 5)
    val Row3 = Row(1003, "Venkat", 10000.0, "Delhi", 5)
    val Row4 = Row(1004, "Sarfaraj", 10000.0, "Kolkata", 5)
    val Row5 = Row(1005, "Manoj", 15000.0, "Banglore", 5)
    val Row6 = Row(1006, "Jasmin", 16000.0, "Mumbai", 5)
    val Row7 = Row(1007, "Reegal", 8000.0, "Banglore", 5)
    val Row8 = Row(1008, "Sayed", 7000.0, "Banglore", 5)
    val Row9 = Row(1005, "Mike", 15000.0, "NewYork", 7)
    val Row10 = Row(1006, "Javier", 14000.0, "Washngton", 3)
    val Row11 = Row(1007, "Ronak", 16000.0, "London", 4)
    val Row12 = Row(1008, "Fiaz", 19000.0, "Balmator", 7)

    val data = Seq(Row1, Row2, Row3, Row4, Row5, Row6, Row7, Row8, Row9, Row10, Row11, Row12)

    val datadf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    //val calc1 = datadf.selectExpr(monotonically_increasing_id(), "*").show()

    //val calc2 = datadf.withColumn("UniqueId",expr("monotonically_increasing_id()")).select("*").show()

    //caps first letter
    val calc2 = datadf.selectExpr("*", "initcap(venue) as venue").show()

    //convert all char in lower or upper case

    val calc3 = datadf.selectExpr("*", "lower(venue) as venue").show()
    val calc4 = datadf.selectExpr("*", "upper(venue) as venue").show()
    val calc5 = datadf.selectExpr("*", "lpad(upper(venue),3)").show()
    val calc6 = datadf.selectExpr("*", "rpad(upper(venue),4)").show()








  }
}