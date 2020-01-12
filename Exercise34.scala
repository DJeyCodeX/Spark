import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise34 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val HEEmployeeDf = Seq((1, "Deva", "Male", 5000, "Sales"),
                               (2, "Jugnu", "Female", 6000, "HR"),
                               (3, "Kavita", "Female", 7500, "IT"),
                               (4, "Vikram", "Male", 6500, "Marketing"),
                               (5, "Shabana", "Female", 5500, "Finance"),
                               (6, "Shantilal", "Male", 8000, "Sales"),
                               (7, "Vinod", "Male", 7200, "HR"),
                               (8, "Vimla", "Female", 6600, "IT"),
                               (9, "Jasmin", "Female", 5400, "Marketing"),
                               (10, "Lovely", "Female", 6300, "Finance"),
                               (11, "Mohan", "Male", 5700, "Sales"),
                               (12, "Purvish", "Male", 7000, "HR"),
                               (13, "Jinat", "Female", 7100, "IT"),
                               (14, "Eva", "Female", 6800, "Marketing"),
                               (15, "Jitendra", "Male", 5000, "Finance"),
                               (15, "Rajkumar", "Male", 4500, "Finance"),
                               (15, "Satish", "Male", 4500, "Finance"),
                               (15, "Himmat", "Male", 3500, "Finance")).toDF("ID",
                                                                             "Name",
                                                                             "Gender",
                                                                             "Salary",
                                                                             "Department")

        val genderPartitionedSpec = Window.partitionBy("Gender").orderBy(expr("Salary").desc_nulls_last)

        HEEmployeeDf.withColumn("Row Number", row_number() over genderPartitionedSpec).show()

        HEEmployeeDf.select(expr("*"), (ntile(3) over genderPartitionedSpec) as "bucketing").show()

        HEEmployeeDf.select(expr("*"), (ntile(4) over genderPartitionedSpec) as "bucketing").show( )

    }
}
