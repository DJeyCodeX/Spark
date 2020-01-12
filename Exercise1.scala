import org.apache.spark.sql.{Encoders, SparkSession}

object Exercise1 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val userDf = spark.createDataFrame(Seq((1001, "Hadoop", 7000),
                                               (1002, "Spark", 8000),
                                               (1003, "Cassandra", 5000),
                                               (1004, "Python", 6000))).toDF("course_id", "course_name", "course_fee")

        val data = Seq(Learner("Amit", "amit@hadoopexam.com", "Mumbai"),
                       Learner("Rakesh", "rakesh@hadoopexam.com", "Pune"),
                       Learner("Jonathan", "jonathan@hadoopexam.com", "NewYork"),
                       Learner("Michael", "michael@hadoopexam.com", "Washington"),
                       Learner("Simon", "simon@hadoopexam.com", "HongKong"),
                       Learner("Venkat", "venkat@hadoopexam.com", "Chennai"),
                       Learner("Roshni", "roshni@hadoopexam.com", "Bangalore"))

        val encodingDf = Encoders.product[Learner]
        println(encodingDf.schema)

        val dataRDD = spark.sparkContext.parallelize(data)

        val DataDf = spark.createDataFrame(dataRDD)
        DataDf.show()

        val DataDs = spark.createDataFrame(dataRDD).as[Learner]
        DataDs.show()
    }

    case class Learner(name: String, Email: String, City: String)

}