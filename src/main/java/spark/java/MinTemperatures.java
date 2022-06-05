package spark.java;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;
import scala.Tuple2;
import java.lang.Math;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;


public class MinTemperatures {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "MinTemperatures");
        JavaRDD<String> lines = sc.textFile("data/1800.csv");
        JavaRDD<Tuple3<String, String, Float>> parsedLines = lines.map((Function<String, Tuple3<String, String, Float>>) line -> {
            String[] fields = line.split(",");
            String stationID = fields[0];
            String entryType = fields[2];
            float temperature = Float.parseFloat(fields[3]) * 0.1f * (9.0f / 5.0f) + 32.0f;
            return new Tuple3<>(stationID, entryType, temperature);
        });
        JavaRDD<Tuple3<String, String, Float>> minTemps = parsedLines.filter(x -> x._2().equals("TMIN"));
        JavaPairRDD<String, Float> stationTemps = minTemps.mapToPair(x -> new Tuple2<>(x._1(), x._3()));
        JavaPairRDD<String, Float> minTempsByStation = stationTemps.reduceByKey(Math::min);
        List<Tuple2<String, Float>> results = minTempsByStation.collect();
        results.stream().sorted(Comparator.comparing(Tuple2::_1)).collect(Collectors.toList()).forEach(
                x -> System.out.println(x._1() + " minimum temperature: " + String.format("%.2f", x._2()) + " F")
        );
    }
}
