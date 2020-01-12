
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object Exercise6 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()


    val df = spark.read.format("json").load("/home/knoldus/Desktop/data.json")

    df.printSchema()


    val schema = StructType(Array(StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("fee", LongType, true),
      StructField("venue", StringType, true),
      StructField("duration", LongType, true)))

    val df1 = spark.read.format("json").schema(schema).load("/home/knoldus/Desktop/data.json")

    df1.printSchema()


  }

}
