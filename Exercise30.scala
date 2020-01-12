
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise30 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val employeeDs = spark.sparkContext.parallelize(Seq(HEEmployee(1, "Deva", "Male", 5000, "Sales"),
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
                                                            HEEmployee(14, "Eva", "Female", 6800, "Marketing"),
                                                            HEEmployee(15, "Jitendra", "Male", 5000, "Finance"),
                                                            HEEmployee(15, "Rajkumar", "Male", 4500, "Finance"),
                                                            HEEmployee(15, "Satish", "Male", 4500, "Finance"),
                                                            HEEmployee(15, "Himmat", "Male", 3500, "Finance"))).toDS()

        val calculateTotalSalary = (sal: Int) => {sal * 20/100} + sal

        val calculateSalaryWithBonus = udf(calculateTotalSalary)
        employeeDs.withColumn("TotalSalary", calculateSalaryWithBonus(expr("Salary"))).show()

        spark.udf.register("TotalSalaryAndBonus",calculateSalaryWithBonus)

        //check whether a udf is registered or not

        println(spark.catalog.listFunctions.filter('name like "%Bonus%").show(false))

        employeeDs.createOrReplaceTempView("temp_view")

        spark.sql("select ID, Name, Gender, Department, TotalSalaryAndBonus(Salary) as TotalSalary from temp_view")
                .show(false)

    }
    case class HEEmployee(ID: Int, Name: String, Gender: String, Salary: Int, Department: String)

}
