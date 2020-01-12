import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise37 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("ERROR")

        val ds1 = spark.sparkContext.parallelize(Seq(HECourse(1, "Hadoop", 6000, "Mumbai", 5),
                                                     HECourse(2, "Spark", 5000, "Pune", 4),
                                                     HECourse(3, "Python", 4000, "Hyderabad", 30),
                                                     HECourse(4, "Scala", 4000, "Kolkata", 3),
                                                     HECourse(5, "HBase", 7000, "Banglore", 7),
                                                     HECourse(4, "Scala", 4000, "Kolkata", 3),
                                                     HECourse(5, "HBase", 7000, "Banglore", 7),
                                                     HECourse(11, "Scala", 4000, "Kolkata", 3),
                                                     HECourse(12, "HBase", 7000, "Banglore", 7))).toDS()

        val ds2 = spark.sparkContext.parallelize(Seq(HECourse(1, "Hadoop", 6000, "Mumbai", 5),
                                                     HECourse(12, "Spark", 5000, "Pune", 4),
                                                     HECourse(13, "Python", 4000, "Hyderabad", 3))).toDS()

        //find all the distinct rows from the dataset
        ds1.distinct().show()

        //drop all duplicate records and keep one record based on column name
        ds1.dropDuplicates("name", "fee", "venue", "duration").show()

        //remove all the records which are common b/w both the dataset
        ds1.except(ds2).show()

        //get all the records from the first dataset where course fee is more than 6000
        ds1.filter("fee > 6000").show()

        ds1.filter(data => data.fee > 6000).show()

        def withTax(dataset: Dataset[HECourse]) = dataset.withColumn("fee_tax", 'fee + 500)

        ds1.transform(withTax).show()

        val HEEcoments = Seq(HEComments(1001, "I wanted to subscribe custom package"),
                             HEComments(1002, " Hi this is lokesh and want to subscribe entire Spark package"))

        //find the unique words across all comments

        HEEcoments.flatMap(_.comment.split(""))

        ds1.union(ds2).sort("id").show()
        ds1.union(ds2).distinct().sort("id").show()
        ds1.union(ds2).distinct().toJSON.show(false)


    }

    case class HECourse(id: Int, name: String, fee: Int, venue: String, duration: Int)

    case class HEComments(id: Long, comment: String)

}