import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object Exercise28 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("Joins")
                    .getOrCreate()
        import spark.implicits._

        val courseStudents = Seq(("Hadoop", 13000, 2011),
                                 ("Spark", 11000, 2013),
                                 ("Cassandra", 12000, 2015),
                                 ("Java", 19000, 2010),
                                 ("Python", 13000, 2009),
                                 ("SQL", 24000, 2009),
                                 ("Scala", 34000, 2013),
                                 ("Hadoop", 12000, 2012),
                                 ("Spark", 12000, 2014),
                                 ("Cassandra", 11000, 2016),
                                 ("Hadoop", 13000, 2011),
                                 ("Spark", 11000, 2013),
                                 ("Cassandra", 12000, 2015),
                                 ("Java", 19000, 2010),
                                 ("Python", 13000, 2009),
                                 ("SQL", 24000, 2009),
                                 ("Scala", 34000, 2013),
                                 ("Hadoop", 12000, 2012),
                                 ("Spark", 12000, 2014),
                                 ("Cassandra", 11000, 2016)).toDF("name", "Students", "year")

        courseStudents.show()

        courseStudents.groupBy("name").pivot("year" ).sum("Students").show()
        courseStudents.groupBy("name").pivot("year" , Seq("2009", "2010", "2012", "2013")).sum("Students").show()


    }
}