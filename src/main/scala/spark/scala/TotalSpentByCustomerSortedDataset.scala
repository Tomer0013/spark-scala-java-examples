package spark.scala

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}
import org.apache.spark.sql.functions._


object TotalSpentByCustomerSortedDataset {

  case class Order (customerID: Int, itemID: Int, amount: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("TotalSpentByCustomerSortedDataset").master("local[*]").getOrCreate()

    val orderSchema = new StructType()
      .add("customerID", IntegerType, true)
      .add("itemID", IntegerType, true)
      .add("amount", FloatType, true)

    import spark.implicits._
    val ds = spark.read
      .schema(orderSchema)
      .csv("data/customer-orders.csv")
      .as[Order]
    val customerSpent = ds.groupBy("customerID")
      .agg(round(sum("amount"), 2).alias("totalSpent"))
      .sort($"totalSpent".desc)

    val results = customerSpent.collect()
    for (result <- results) {
      println(f" Customer ${result(0)}: ${result(1)}")
    }
    spark.stop()
  }

}
