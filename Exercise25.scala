import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Exercise25 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                  .load("/home/knoldus/Downloads/hadoopexamdatasets/data.csv")

        val df1ds = df1.as[HECourse]

        val ds1 = df1ds.createOrReplaceTempView("temp_view")

        spark.sql("cache table temp_view")

        println(spark.catalog.isCached("temp_view"))

        spark.sql("clear cache")

        spark.catalog.cacheTable("temp_view", StorageLevel.MEMORY_AND_DISK)

        //it is required to cache the table
        spark.sql("select * from temp_view")

        spark.sql("select sum(Fee) as TotalFee, Venue from temp_view group by venue").show()


        //left questions

    }
    case class HECourse(ID: Int, Name: String, Fee: Int, Venue: String, Date: String, Duration: Int)

}
