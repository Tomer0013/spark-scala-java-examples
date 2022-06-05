package spark.scala

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline


object DecisionTreeDataset {

  case class realEstateSchema(No: Int,
                              TransactionDate: Double,
                              HouseAge: Double,
                              DistanceToMRT: Double,
                              NumberConvenienceStores: Int,
                              Latitude: Double,
                              Longitude: Double,
                              PriceOfUnitArea: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark = SparkSession.builder().appName("DecisionTreeExample").master("local[*]").getOrCreate()

    // Get data
    import spark.implicits._
    val dsRaw = spark.read.option("sep", ",").option("inferSchema", true).option("header", true)
      .csv("./data/realestate.csv").as[realEstateSchema]

    // Build pipeline
    val feat_cols = dsRaw.columns.filterNot(Array("No", "PriceOfUnitArea").contains)
    val assembler = new VectorAssembler().setInputCols(feat_cols).setOutputCol("features")
    val dt = new DecisionTreeRegressor().setLabelCol("PriceOfUnitArea").setFeaturesCol("features")
    val pipeline = new Pipeline().setStages(Array(assembler, dt))

    // Split to train test, fit and predict
    val Array(trainData, testData) = dsRaw.randomSplit(Array(0.7, 0.3))
    val model = pipeline.fit(trainData)
    val preds = model.transform(testData)

    // Evaluate with RMSE
    val eval = new RegressionEvaluator().setLabelCol("PriceOfUnitArea").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = eval.evaluate(preds)
    println(s"RMSE on test data: $rmse")

    spark.stop()
  }
}
