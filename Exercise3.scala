import org.apache.spark.sql.{Encoders, SparkSession}



object Exercise3 {
  case class HECourse(id:Int, name:String, fee: Int, venue:String, duration:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val datads = spark.sparkContext.parallelize(Seq(HECourse(1, "Hadoop", 6000, "Mumbai", 5),
      HECourse(2, "Spark", 5000, "Pune", 4),
      HECourse(3, "Python", 4000, "Hyderabad", 3),
      HECourse(4, "Scala", 4000, "Kolkata", 3), HECourse(5, "HBase", 7000, "Bangalore", 7)))

    val datadss = datads.toDS

    datadss.filter('fee > 5000).show()



  }
}
