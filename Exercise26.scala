import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise26 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                  .load("/home/knoldus/Downloads/hadoopexamdatasets/data2.csv")

        val df2 = df1.groupBy("Name", "Venue").agg(sum("Fee") as "TotalFee").selectExpr("Venue", "Name", "TotalFee").show()
        val df3 = df1.groupBy( "Venue").agg(sum("Fee") as "TotalFee").selectExpr("Venue","TotalFee").show()
        val df4 = df1.groupBy( "Name").agg(sum("Fee") as "TotalFee").selectExpr("Name","TotalFee").show(false)
        val df5 = df1.sort(expr("Name").asc_nulls_first).show(false)
        df1.createOrReplaceTempView("temp_view")

        //sql queries left



    }
}
