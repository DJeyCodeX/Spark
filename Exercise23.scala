import org.apache.spark.sql.SparkSession

object Exercise23 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

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
                                                     HECourse(12, "Spark", 5000,"Pune", 4),
                                                     HECourse(13, "Python", 4000, "Hyderabad", 3))).toDS()

        val join = ds1.joinWith(ds2, ds1("ID") === ds2("ID")).show(false)
        val leftjoin = ds1.joinWith(ds2, ds1("ID") === ds2("ID"), "left").show(false)
        val rightjoin = ds1.joinWith(ds2, ds1("ID") === ds2("ID"), "right").show(false)
    }

    case class HECourse(id: Int, name: String, fee: Int, venue: String, duration: Int)
}
