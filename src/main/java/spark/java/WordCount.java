package spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "WordCount");
        JavaRDD<String> lines = sc.textFile("./data/book.txt");
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        Map<String, Long> wordCounts = words.countByValue();
        wordCounts.forEach((k, v) -> {
           System.out.println("(" + k + ", " + v + ")");
        });
    }
}
