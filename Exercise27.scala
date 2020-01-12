import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise27 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val employeeds = spark.sparkContext.parallelize(Seq(HEEmployee(1, "Deva", "Male", 5000, "Sales"),
                                                            HEEmployee(2, "Jugnu", "Female", 6000, "HR"),
                                                            HEEmployee(3, "Kavita", "Female", 7500, "IT"),
                                                            HEEmployee(4, "Vikram", "Male", 6500, "Marketing"),
                                                            HEEmployee(5, "Shabana", "Female", 5500, "Finance"),
                                                            HEEmployee(6, "Shantilal", "Male", 8000, "Sales"),
                                                            HEEmployee(7, "Vinod", "Male", 7200, "HR"),
                                                            HEEmployee(8, "Vimla", "Female", 6600, "IT"),
                                                            HEEmployee(9, "Jasmin", "Female", 5400, "Marketing"),
                                                            HEEmployee(10, "Lovely", "Female", 6300, "Finance"),
                                                            HEEmployee(11, "Mohan", "Male", 5700, "Sales"),
                                                            HEEmployee(12, "Purvish", "Male", 7000, "HR"),
                                                            HEEmployee(13, "Jinat", "Female", 7100, "IT"),
                                                            HEEmployee(14, "Eva", "Female", 6800,"Marketing"),
                                                            HEEmployee(15, "Jitendra", "Male", 5000, "Finance"))).toDS()

        employeeds.printSchema()

        employeeds.createOrReplaceTempView("temp_view")

        spark.sql("select * from temp_view").show()

        spark.sql("select department, sum(salary) as Total_salary from temp_view group by department").show(false)

        //rollup create a row called 'all department' and added sum of all departments
        spark.sql("select coalesce( department, 'All Departments') as department, coalesce( gender, 'All Genders') as" +
                  " department, sum(salary) as Total_salary from temp_view group by rollup (department, gender)").orderBy("department").show(false)

        spark.sql("select coalesce( department, 'All Departments') as department, coalesce( gender, 'All " +
                  "Genders') as department, sum(salary) as Total_salary from " +
                  "temp_view group by cube (department, gender)").orderBy("department").show(false)

        //error in last 2 queries check again





    }
    case class HEEmployee(ID: Int, Name: String, Gender: String, Salary: Int, Department: String)


}
