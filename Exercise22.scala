import org.apache.spark.sql.SparkSession

object Exercise22 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()

        import spark.implicits._

        val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                      .load("/home/knoldus/Downloads/hadoopexamdatasets/data.csv")

        val df2 = spark.sparkContext.parallelize(Seq((1, "Hadoop", 6000, "Mumbai", 5),
                                                        (2, "Spark", 5000, "Pune", 4),
                                                        (3, "Python", 4000, "Hyderabad", 3)))
                .toDF("ID", "Name", "Fee","City", "Days")

        val dfjoin = df1.join(df2, "ID").show()

        val leftjoin = df1.join(df2, Seq("ID"), "left").show()
        val rightjoin = df1.join(df2, Seq("ID"), "right").show()
        val fullouterjoin = df1.join(df2, Seq("ID"), "fullouter").show()
        //val broadcastedjoin = df1.join((df2, Seq("ID")).show()


    }
}
