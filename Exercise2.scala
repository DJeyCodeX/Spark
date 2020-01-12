import org.apache.spark.sql.SparkSession

object Exercise2 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val data = Seq(Learner("Amit", "amit@hadoopexam.com", "Mumbai"),
                       Learner("Rakesh", "rakesh@hadoopexam.com", "Pune"),
                       Learner("Jonathan", "jonathan@hadoopexam.com", "NewYork"),
                       Learner("Michael", "michael@hadoopexam.com", "Washington"),
                       Learner("Simon", "simon@hadoopexam.com", "HongKong"),
                       Learner("Venkat", "venkat@hadoopexam.com", "Chennai"),
                       Learner("Roshni", "roshni@hadoopexam.com", "Bangalore"))

        val dataRDD = spark.sparkContext.parallelize(data)

        val DataDf = spark.createDataFrame(dataRDD)

        DataDf.write.parquet("/home/knoldus/Downloads/assignment.parquet")

        val ds = spark.read.parquet("/home/knoldus/Downloads/assignment.parquet")
        ds.show()

    }
    case class Learner(name: String, Email: String, City: String)
}
