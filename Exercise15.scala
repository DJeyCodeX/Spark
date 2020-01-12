import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise15 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        val schema = StructType(Array(StructField("id", IntegerType, true),
                                      StructField("name", StringType, true),
                                      StructField("fee", DoubleType, true),
                                      StructField("venue", StringType, true),
                                      StructField("Duration", IntegerType, true),
                                      StructField("Detail", StringType, true)))

        val Row1 = Row(1001, "Amit Kumar", 10000.0, "Mumbai", 5, "Hadoop and Spark Training by HadoopExam.com")
        val Row2 = Row(1002, "John", 10000.0, "Mumbai", 5, "AWS Training by HadoopExam.com")
        val Row3 = Row(1003, "Venkat", 10000.0, "Delhi", 5, "Cassandra Training by HadoopExam.com")
        val Row4 = Row(1004, "Sarfraj", 10000.0, "Kolkata", 5, "Java and Python Training by HadoopExam.com")
        val Row5 = Row(1005, "Manoj", 11000.0, "Banglore", 5, "FinTech Training by HadoopExam.com")
        val Row6 = Row(1006, "Jasmin", 11000.0, "Mumbai", 5, "IOT Training by HadoopExam.com")
        val Row7 = Row(1007, "Reegal", 11000.0, "Banglore", 5, "Hadoop and Spark Training by HadoopExam.com")
        val Row8 = Row(1008, "Sayed", 11000.0, "Banglore", 5, "Hadoop and Spark Training by HadoopExam.com")
        val Row9 = Row(1009, "Mike", 15000.0, "Newyork", 7, "Hadoop and Spark Training by HadoopExam.com")
        val Row10 = Row(1010, "Javier", 14000.0, "Washington", 3, "Hadoop and Spark Training by HadoopExam.com")
        val Row11 = Row(1011, "Ronak", 16000.0, "London", 4, "Hadoop and Spark Training by HadoopExam.com")
        val Row12 = Row(1012, "Fiaz", 19000.0, "Baltimor", 7, "AWS Training by QuickTechie.com")
        val Row13 = Row(1013, "Vikram", 19000.0, "Baltimor", 7, "Cassandra Training by QuickTechie.com")
        val Row14 = Row(1014, "Deepak", 19000.0, "Baltimor", 7, "Java Training by QuickTechie.com")
        val Row15 = Row(1015, "Venugopal", 19000.0, "Baltimor", 7, "Oracle DBA Training by QuickTechie.com")
        val Row16 = Row(1016, "Shankar", 19000.0, "Baltimor", 7, "Oracle DBA Training by QuickTechie.com")
        val Row17 = Row(1017, "Rohit", null, "Baltimor", 7, "Oracle DBA Training by QuickTechie.com")
        val Row18 = Row(1018, "Ranu", 19000.0, null, 7, null)
        val Row19 = Row(1019, "Diksha", 19000.0, "Baltimor", 7, null)
        val Row20 = Row(null, null, null, null, null, null)

        val data = Seq(Row1, Row2, Row3, Row4, Row5, Row6, Row7, Row8, Row9, Row10, Row11, Row12, Row13, Row14,
                       Row15, Row16, Row17, Row18, Row19, Row20)

        val datadf = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        val df = datadf.withColumn("Venue", when(expr("Venue<> ''"), expr("Venue")).otherwise("Mumbai")).show()

        val df2 = datadf.na.drop("all").show()

        val df3 = datadf.na.drop("all", Seq("venue", "Detail")).show()

        //drop all null values in data
        val df4 = datadf.na.drop("any").show()

        //replace all null values with string ~~~
        val df5 = datadf.na.fill("~~~").show()

        val df6 = datadf.na.fill(Map("Fee" -> 10000, "Name"-> "Auditor", "venue" -> "Mumbai", "Duration" -> 5,
                                     "Detail" -> "Course From HadoopExam.com"))

        val df7 = df6.withColumn("venue", when(expr("venue").equalTo("Mumbai"), "Pune").otherwise(col("venue"))).show()


    }
}

