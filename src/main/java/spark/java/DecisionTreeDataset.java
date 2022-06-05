package spark.java;

import org.apache.log4j.*;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.*;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

import java.io.Serializable;
import java.util.Arrays;


public class DecisionTreeDataset {

    public static class RealEstateRow implements Serializable{
        private int No;
        private double TransactionDate;
        private double houseAge;
        private double DistanceToMRT;
        private int NumberConvenienceStores;
        private double Latitude;
        private double Longitude;
        private double PriceOfUnitArea;

        public int getNo() {
            return No;
        }

        public void setNo(int no) {
            No = no;
        }

        public double getTransactionDate() {
            return TransactionDate;
        }

        public void setTransactionDate(double transactionDate) {
            TransactionDate = transactionDate;
        }

        public double getHouseAge() {
            return houseAge;
        }

        public void setHouseAge(double houseAge) {
            this.houseAge = houseAge;
        }

        public double getDistanceToMRT() {
            return DistanceToMRT;
        }

        public void setDistanceToMRT(double distanceToMRT) {
            DistanceToMRT = distanceToMRT;
        }

        public int getNumberConvenienceStores() {
            return NumberConvenienceStores;
        }

        public void setNumberConvenienceStores(int numberConvenienceStores) {
            NumberConvenienceStores = numberConvenienceStores;
        }

        public double getLatitude() {
            return Latitude;
        }

        public void setLatitude(double latitude) {
            Latitude = latitude;
        }

        public double getLongitude() {
            return Longitude;
        }

        public void setLongitude(double longitude) {
            Longitude = longitude;
        }

        public double getPriceOfUnitArea() {
            return PriceOfUnitArea;
        }

        public void setPriceOfUnitArea(double priceOfUnitArea) {
            PriceOfUnitArea = priceOfUnitArea;
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("DecisionTreeExample").master("local[*]").getOrCreate();

        // Get data
        Encoder<RealEstateRow> realEstateRowEncoder = Encoders.bean(RealEstateRow.class);
        Dataset<RealEstateRow> dsRaw = spark.read().option("sep", ",").option("header", true).option("inferSchema", true)
                .csv("./data/realestate.csv").as(realEstateRowEncoder);

        // Build pipeline
        String[] feat_cols = Arrays.stream(dsRaw.columns()).filter(x -> (!x.equals("No") && (!x.equals("PriceOfUnitArea")))).toArray(String[]::new);
        VectorAssembler assembler = new VectorAssembler().setInputCols(feat_cols).setOutputCol("features");
        DecisionTreeRegressor dt = new DecisionTreeRegressor().setLabelCol("PriceOfUnitArea").setFeaturesCol("features");
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler, dt});

        // Split to train test, fit and predict
        Dataset<RealEstateRow>[] trainTest = dsRaw.randomSplit(new double[]{0.7, 0.3});
        Dataset<RealEstateRow> trainDs = trainTest[0];
        Dataset<RealEstateRow> testDs = trainTest[1];
        PipelineModel model = pipeline.fit(trainDs);
        Dataset<Row> preds = model.transform(testDs);

        // Evaluate with RMSE
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("PriceOfUnitArea").setPredictionCol("prediction")
                        .setMetricName("rmse");
        double rmse = evaluator.evaluate(preds);
        System.out.println("RMSE on test set " + rmse);

        spark.stop();
    }

}
