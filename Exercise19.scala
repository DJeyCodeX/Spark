import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Exercise19 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val datacsv = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                      .load("/home/knoldus/Downloads/hadoopexamdatasets/noheader.csv").show()

        val schema= StructType(Array(StructField("id", IntegerType, false),
                                     StructField("name", StringType, false),
                                     StructField("fee", LongType, false),
                                     StructField("venue", StringType, false),
                                     StructField("data", StringType, false)))

        spark.read.schema(schema).csv("/home/knoldus/Downloads/hadoopexamdatasets/noheader.csv").show()


        /*spark.read.schema(schema).csv("/home/knoldus/Downloads/hadoopexamdatasets/noheader.csv")
                .write.option("compressed", "gzip").save("/home/knoldus/Downloads/hadoopexamdatasets/1")*/

        spark.read.load("/home/knoldus/Downloads/hadoopexamdatasets/1").show()
    }
}
