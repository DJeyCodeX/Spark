import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise32 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val HEEmployeeDf = spark.sparkContext.parallelize(Seq((1, "Deva", "Male", 5000, "Sales"),
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
                                                            (15, "Himmat", "Male", 3500, "Finance"))).toDF("ID", "Name", "Gender", "Salary", "Department")

        //create a window based on the gender to rank their salary
        //For the same salary it will assgn the same rank
        //As we wanted to give rank the salary under the same gender. Hence window should be created on gender
        //Also wanted to make sure null salary shoul have lowest rank

        val genderPartitionedSpec = Window.partitionBy("Gender").orderBy(expr("Salary").desc_nulls_last)

        HEEmployeeDf.withColumn("rank", rank over genderPartitionedSpec).show()

        //the spark sql rank analytic function is used to get rank of the rows in columns or within group
        //the rows with equal or similar values recieve the same rank and next rank would be skipped
        //create a window based on department to rank their salary
        //Rank the salary for each employee in a department

        val DepartmentgenderPartitionedSpec = Window.partitionBy("Department", "Gender").orderBy(expr("Salary")
                                                                                                  .desc_nulls_last)

        HEEmployeeDf.withColumn("rank", rank over DepartmentgenderPartitionedSpec).show()

        //create a window based on the department as well as gender to rank their salary
        //sso that we can rank the salary for each gender in a department

        val genderPartitionedSpecPercent = Window.partitionBy("Gender").orderBy(expr("Salary").desc_nulls_last)

        HEEmployeeDf.withColumn("PercentRank", percent_rank() over genderPartitionedSpecPercent).show(false)

        val genderPartitionedSpecdense = Window.partitionBy("Gender").orderBy(expr("Salary").desc_nulls_last)

        HEEmployeeDf.withColumn("DenseRank", dense_rank() over genderPartitionedSpecdense).show(false)





    }

}

