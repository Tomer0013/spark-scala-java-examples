package spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountBetterSorted {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "WordCountBetterSorted");
        JavaRDD<String> lines = sc.textFile("./data/book.txt");
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split("\\W+")).iterator());
        JavaRDD<String> lowerCaseWords = words.map(String::toLowerCase);
        JavaPairRDD<String, Integer> wordCounts = lowerCaseWords.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String> wordCountsSorted = wordCounts.mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey(true, 1);
        wordCountsSorted.foreach(x -> {
            System.out.println(x._2() + ": " + x._1());
        });
    }
}
