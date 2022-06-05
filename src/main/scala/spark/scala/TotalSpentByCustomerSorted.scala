package spark.scala

import org.apache.spark._
import org.apache.log4j._

object TotalSpentByCustomerSorted {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val amount = fields(2).toFloat
    (customerID, amount)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
    val lines = sc.textFile("./data/customer-orders.csv")
    val customerSpent = lines.map(parseLine).reduceByKey((v1, v2) => v1 + v2)
    val customerSpentSorted = customerSpent.map(x => (x._2, x._1)).sortByKey(ascending = false, 1)
    val results = customerSpentSorted.collect()
    results.foreach(x => println(f"Customer ${x._2}: ${x._1}%.2f"))
  }

}
