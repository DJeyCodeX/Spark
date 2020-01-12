import org.apache.spark.sql.SparkSession

object Exercise24 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                  .load("/home/knoldus/Downloads/hadoopexamdatasets/data.csv")

        //df1.show(false)

        val df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                  .load("/home/knoldus/Downloads/hadoopexamdatasets/data1.csv")

        //df2.show(false)

        val df1ds = df1.as[HECourse]

        //df1ds.show(false)

        val df2new = df2.toDF("ID", "LearnersCount", "Website")

        //df2new.show(false)

        val df2ds = df2new.as[HEStats]

        //df2ds.show(false)

        //do the join and apply hint as broadcast
        df1ds.join(df2ds.hint("Broadcast"), "ID").show()

        //check whether the hint is resolved or not
        println(df1ds.join(df2ds.hint("Broadcast"), "ID").queryExecution.logical)

        //to check current smaller filee size threshold
        println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt)

        //left something
    }

    case class HECourse(ID: Int, Name: String, Fee: Int, Venue: String, Date: String, Duration: Int)

    case class HEStats(ID: Int, LearnersCount: String, Website: String)

}
