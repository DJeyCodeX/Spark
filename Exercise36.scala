import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise36 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val heds = spark.sparkContext.parallelize(Seq(HECourse(1, "Hadoop", 6000, "Mumbai", 5),
                                                              HECourse(2, "Spark", 5000, "Pune", 4),
                                                              HECourse(3, "Python", 4000, "Hyderabad", 3)
                                                              ), 3).toDS()

        println("Number of partitions:" + heds.rdd.partitions.size)

        val hedsnew = heds.repartition(1)

        println("Number of partitions:" + hedsnew.rdd.partitions.size)


        //create dataset with 3 partitions

        val heds2 = spark.sparkContext.parallelize(Seq(HECourse(1, "Hadoop", 6000, "Mumbai", 5),
                                                      HECourse(2, "Spark", 5000, "Pune", 4),
                                                      HECourse(3, "Python", 4000, "Hyderabad", 3)
                                                      ), 3).toDS()


        println("Number of partitions:" + heds2.rdd.partitions.size)

        val hedsnew1 = heds2.coalesce(1)

        println("Number of partitions:" + hedsnew1.rdd.partitions.size)

        val hedsnew2 = heds2.coalesce(2)

        println("Number of partitions:" + hedsnew2.rdd.partitions.size)

    }

    case class HECourse(id: Int, name: String, fee: Int, venue: String, duration: Int)

}