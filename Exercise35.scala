import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise35 {

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

        //lag function will help you find the previous value in same column

        //find the immediate salary for each empployee for individual gender across department

        HEEmployeeDf.withColumn("previousValue", lag("Salary",1) over genderPartitionedSpec).show()

        //2nd highest salary
        HEEmployeeDf.withColumn("previousValue", lag("Salary",2) over genderPartitionedSpec).show()

        //3RD HIGHEST SALARY
        HEEmployeeDf.withColumn("previousValue", lag("Salary",3) over genderPartitionedSpec).show()

        //GET THE DIFFERENCE B/E PREVIOUS VALUE AND CURRENT VALUE

        //FIND THE diff b/w immediate higher salary and current employee slaary

        HEEmployeeDf.withColumn("previousValue", lag("salary", 1) over genderPartitionedSpec)
                .select('ID, 'Name, 'Gender, 'Salary, 'Department, 'previousvalue, ('Salary- 'previousValue ) as
                                                                                   "salaryDiff").show()

        //find the immediate lower salary for each empployee for individual gender across department

        HEEmployeeDf.withColumn("leadValue", lead("Salary",1) over genderPartitionedSpec).show()
        HEEmployeeDf.withColumn("leadValue", lead("Salary",2) over genderPartitionedSpec).show()


    }
}
