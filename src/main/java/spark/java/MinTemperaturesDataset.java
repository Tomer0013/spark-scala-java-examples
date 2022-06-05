package spark.java;

import org.apache.spark.sql.*;
import org.apache.log4j.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class MinTemperaturesDataset {

    public static class Temperature implements Serializable {
        private String stationID;
        private int date;
        private String measure_type;
        private long temperature;

        public String getStationID() {
            return stationID;
        }

        public void setStationID(String stationID) {
            this.stationID = stationID;
        }

        public int getDate() {
            return date;
        }

        public void setDate(int date) {
            this.date = date;
        }

        public String getMeasure_type() {
            return measure_type;
        }

        public void setMeasure_type(String measure_type) {
            this.measure_type = measure_type;
        }

        public long getTemperature() {
            return temperature;
        }

        public void setTemperature(long temperature) {
            this.temperature = temperature;
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MinTemperaturesDataset").master("local[*]").getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("stationID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("date", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("measure_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("temperature", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(fields);

        Encoder<Temperature> temperatureEncoder = Encoders.bean(Temperature.class);
        Dataset<Temperature> ds = spark.read().schema(schema).csv("./data/1800.csv").as(temperatureEncoder);

        Dataset<Temperature> minTemps = ds.filter(col("measure_type").equalTo("TMIN"));
        Dataset<Row> stationTemps = minTemps.select("stationID", "temperature");
        Dataset<Row> minTempsByStation = stationTemps.groupBy("stationID").min("temperature");
        Dataset<Row> minTempsByStationF = minTempsByStation.withColumn("temperature", round(col("min(temperature)")
                .multiply(0.1f * (9.0f / 5.0f)).$plus(32.0f), 2))
                .select("stationID", "temperature").sort("temperature");

        List<Row> results = minTempsByStationF.collectAsList();
        for(Row x: results) {
            System.out.println(x.get(0) + " minimum temperature: " + String.format("%.2f", x.get(1)) + " F");
        }

        spark.stop();
    }
}
