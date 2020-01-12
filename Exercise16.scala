import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise16 {

    case class HECourse(id: String, name: String, fee: String, venue: String, duration: String)
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val files = spark.read.format("json").option("inferSchema", "True").load("/home/knoldus/Downloads/hadoopexamdatasets/data_he_data_1.json",
                                                   "/home/knoldus/Downloads/hadoopexamdatasets/data_he_data_2.json")
        files.show()
        files.printSchema()
        println(files.inputFiles(0))
        println(files.inputFiles(1))

        println("Whether data is present in local ?" + files.isLocal)

        println(files.columns)
        println(files.dtypes)
        files.printSchema()

        files.createTempView("localview")
        files.createGlobalTempView("globalview")

        spark.sql("select * from localview").show()
        spark.sql("select * from global_temp.globalview").show()

        //renaming the columns

        files.toDF("NUMBEROFDAYS", "FIXEDFEE", "COURSE_ID", "COURSE_NAME", "TRAINING_VENUE").printSchema()

        files.as[HECourse].map(_.getClass.getName).show(false)



    }
}

