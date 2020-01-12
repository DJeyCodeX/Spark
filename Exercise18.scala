import org.apache.spark.sql.SparkSession

object Exercise18 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val datacsv = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                .load("/home/knoldus/Downloads/hadoopexamdatasets/data.csv").show()

        val datajson = spark.read.format("json").option("header", "true").option("inferSchema", "true")
                                                .load("/home/knoldus/Downloads/hadoopexamdatasets/data1.json").show()

        //datacsv.printSchema()
        //datajson.printSchema()
        val datatext = spark.read.textFile("/home/knoldus/Downloads/hadoopexamdatasets/data.csv").map(_.split(","))
                .show(false)


    }
}
