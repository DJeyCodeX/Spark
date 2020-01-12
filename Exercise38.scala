import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

object Exercise38 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        //defining a custom schema

        val schema = StructType(Array(StructField("Name", StringType),
                                      StructField("Trainer", StringType),
                                      StructField("Fee", StringType)))

        val trainingSchema = StructType(Array(StructField("TrainingLocation", StringType),
                                              StructField("Courses", schema)))

        trainingSchema.printTreeString()

        val trainingjson = trainingSchema.json

        println(trainingSchema.prettyJson)

        //check whether encoded schema of json is ok or not

        val jsondatatype = DataType.fromJson(trainingjson)

        //print in sql type

        println(jsondatatype.sql)

        val coursesJson = Seq("""{
                                |    "TrainingLocation" : "Mumbai",
                                |    "courses" : [
                                |      {
                                |        "Name" : "Hadoop",
                                |        "Trainer" : "Nitin Sharma",
                                |        "Fee" : "7000"
                                |      },
                                |      {
                                |        "Name" : "Spark",
                                |        "Trainer" : "Venkat",
                                |        "Fee" : "8000"
                                |      },
                                |       {
                                |        "Name" : "Cassandra",
                                |        "Trainer" : "Hemendra Jain",
                                |        "Fee" : "7000"
                                |      },
                                |      {
                                |        "Name" : "SparkSQL",
                                |        "Trainer" : "Rajkumar Jain",
                                |        "Fee" : "8000"
                                |      }
                                |    ]
                                |  }
                                |""").toDF("coursesJson")



    }
}
