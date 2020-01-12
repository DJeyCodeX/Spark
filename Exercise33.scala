import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise33 {

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

        //cumulative distance window spec

        //find out by how much percent all other female has less or equal salary then EVA

        val cumulativeWindowSpec = Window.partitionBy("Gender").orderBy("Salary")

        val cumDistWindowSpec = HEEmployeeDf.withColumn("cumDist", cume_dist() over cumulativeWindowSpec).show()

        //find out by how much percent all other male empployee in Finance Department has less than or equal to satish Salary

        val cumulativeWindowSpec1 = Window.partitionBy("Department", "Gender").orderBy("Salary")

        val cumDistWindowSpec1 = HEEmployeeDf.withColumn("cumDist", cume_dist() over cumulativeWindowSpec1).show()
    }
}

