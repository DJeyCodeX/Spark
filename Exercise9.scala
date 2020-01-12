import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise9 {
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

    val data = Seq(Row1, Row2, Row3, Row4, Row5, Row6, Row7, Row8)

    val datadf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    println(datadf.rdd.getNumPartitions)

    println(datadf.rdd.repartition(5).getNumPartitions)

    println(datadf.repartition(3, expr("venue")).rdd.getNumPartitions)

    println(datadf.repartition(5, expr("venue")).coalesce(2).rdd.getNumPartitions)

  }
}

