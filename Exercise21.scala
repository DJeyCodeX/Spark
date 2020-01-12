import org.apache.spark.sql.SparkSession

object Exercise21 {
    case class HECourse(id: Int, name: String, fee: Int, venue: String,Date: String, duration: String)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        import spark.implicits._

        val datacsv = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                      .load("/home/knoldus/Downloads/hadoopexamdatasets/data.csv")

        val datads = datacsv.as[HECourse]

        datads.printSchema()

        datads.cache()

        //datads.show()

        println("******\n" + datads.queryExecution.withCachedData + "\n*******")

        datads.unpersist()

        spark.sparkContext.setCheckpointDir("/home/knoldus/Downloads/hadoopexamdatasets/3")

        println(spark.sparkContext.getCheckpointDir.get)

        println(datads.queryExecution.toRdd.toDebugString)

        datads.toDF().show()


    }
}
