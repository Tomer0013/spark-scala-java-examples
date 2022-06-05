package spark.java;

import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.Comparator;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

public class RatingsCounter {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RatingsCounter");
        JavaRDD<String> lines = sc.textFile("data/ml-100k/u.data", 1);
        JavaRDD<String> ratings = lines.map(x -> x.split("\t")[2]);
        Map<String, Long> results = ratings.countByValue();
        List<Map.Entry<String, Long>> sortedResults = results.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList());
        sortedResults.forEach(x -> {
            System.out.println("(" + x.getKey() + ", " + x.getValue() + ")");
        });
    }
}
