import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Joins_3 {
  def main(args: Array[String]): Unit = {

     val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Joins")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val payment = spark.createDataFrame(Seq(
      (1, 101, 2500), (2, 102, 1110), (3, 103, 500), (4, 104, 400), (5, 105, 150), (6, 106, 450)
    )).toDF("paymentId", "customerId", "amount")

    val customer = spark.createDataFrame(Seq(
      (101, "Jon"), (102, "Aron"), (103, "Sam"))).toDF("customerId", "name")

    //Inner JoincreateDataFrame
    //val InnerJoinDF = customer.join(payment, "customerId").show()

    //Left Join
    //val leftJoinDf = payment.join(customer,Seq("customerId"), "left").show()
    //val leftJoinDf1 = payment.join(customer,Seq("customerId"), "left_outer").show()
    //val leftJoinDf = customer.join(payment,Seq("customerId"), "left").show()

    //Right Join
    //val leftJoinDf = payment.join(customer,Seq("customerId"), "right").show()
    //val leftJoinDf1 = payment.join(customer,Seq("customerId"), "right_outer").show()
    //val leftJoinDf2 = customer.join(payment,Seq("customerId"), "right").show()

    //Outer Join
    //val fullJoinDf = payment.join(customer, Seq("customerId"), "outer").show()
    //val fullJoinDf1 = customer.join(payment, Seq("customerId"), "outer").show()

    //Cross Join
    //val crossJoinDf = customer.crossJoin(payment).show()

    //LeftSemi Join
    //val leftsemijoindf = payment.join(customer, payment("customerId") === customer("customerId"), "leftsemi").show()
    //val leftsemijoindf1 = customer.join(payment, customer("customerId") === payment("customerId"), "leftsemi").show()

    //LeftAntiSemi Join
    //val leftsemijoindf = payment.join(customer, payment("customerId") === customer("customerId"), "leftanti").show()
    //val leftsemijoindf1 = customer.join(payment, customer("customerId") === payment("customerId"), "leftanti").show()

    //Self Join
    val employee1 = spark.createDataFrame(Seq(
      (1,"ceo",None),
      (2,"manager1",Some(1)),
      (3,"manager2",Some(1)),
      (101,"Amy",Some(2)),
      (102,"Sam",Some(2)),
      (103,"Aron",Some(3)),
      (104,"Bobby",Some(3)),
      (105,"Jon", Some(3))
    )).toDF("employeeId","employeeName","managerId")

    val selfJoinedEmp = employee1.as("e")
      .join(employee1.as("m"),col("m.employeeId") === col("e.managerId")).show()

  }
}
