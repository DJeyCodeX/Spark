
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Exercise7 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    val schema = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("fee", DoubleType, true),
      StructField("venue", StringType, true),
      StructField("duration", IntegerType, true)))


    //create temporary data using row object
    val Row1 = Row(1001, "Amit", 10000.0, "Mumbai", 5)
    val Row2 = Row(1002, "John", 10000.0, "Mumbai", 5)
    val Row3 = Row(1003, "Venkat", 10000.0, "Delhi", 5)
    val Row4 = Row(1004, "Sarfaraj", 10000.0, "Kolkata", 5)

    val data = Seq(Row1, Row2, Row3, Row4)

    val datadf = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))

    datadf.show()

    datadf.select("name").show()
    datadf.select("id", "name").show()
    datadf.select(expr("id As COURSE_ID" )).show()
    datadf.selectExpr("avg(fee)").show()

  }
  }
