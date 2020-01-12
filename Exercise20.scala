import org.apache.spark.sql.SparkSession

object Exercise20 {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val datajson = spark.read.format("json").load("/home/knoldus/Downloads/hadoopexamdatasets/data1.json")

        datajson.printSchema()

        datajson.createOrReplaceTempView("tempview")

        spark.catalog.tableExists("tempview")

        val table = spark.read.table("tempview")
        table.show()

        println("Total record count" + spark.sql("select * from tempview").count())

        println("Average fee collected " + spark.sql("select avg(fee) from tempview").show())
    }
}
