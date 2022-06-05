package spark.scala

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object MostObscureSuperheroesDataset {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MostObscureSuperheroesDataset")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    // Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val minConnections = connections.sort("connections").first()(1)
    val heroesWithMinConnections = connections.filter($"connections" === minConnections)
      .join(names, "id")

    heroesWithMinConnections.select("name", "connections").show(heroesWithMinConnections.count.toInt)

    spark.stop()
  }
}