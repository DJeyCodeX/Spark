import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise40 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val employeeds = Seq((1, "Deva", "Male", 5000, "Sales"),
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
                                                            (14, "Eva", "Female", 6800,"Marketing"),
                                                            (15, "Jitendra", "Male", 5000, "Finance"))
                .toDF("ID", "Name", "Gender", "Salary", "Department")

        employeeds.show()

        val maleexpr = expr("gender = 'Male'")
        val femaleexpr = expr("gender = 'Female'")
        val salary = expr("Salary >= '6600'")

        employeeds.filter(maleexpr).show()
        employeeds.filter(femaleexpr).show()
        employeeds.filter(salary).show()

        employeeds.filter(salary).withColumn("Array", array("Name", "Gender", "Department"))
                .drop("Name", "Gender", "Department").show(false)

        employeeds.filter(salary).withColumn("Struct", struct("Name", "Gender", "Department"))
        .drop("Name", "Gender", "Department").show(false)

        employeeds.withColumn("uniqueID", monotonically_increasing_id()).show()



    }

    case class HEEmployee(ID: Int, Name: String, Gender: String, Salary: Int, Department: String)

}