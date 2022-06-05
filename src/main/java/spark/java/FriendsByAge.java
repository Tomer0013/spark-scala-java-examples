package spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;


class ParseLine implements PairFunction<String, Integer, Integer> {
    public Tuple2<Integer, Integer> call(String line) {
        String[] fields = line.split(",");
        int age = Integer.parseInt(fields[2]);
        int numFriends = Integer.parseInt(fields[3]);
        Tuple2<Integer, Integer> tuple = new Tuple2(age, numFriends);
        return tuple;
    }
}

public class FriendsByAge {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "FriendsByAge");
        JavaRDD<String> lines = sc.textFile("data/fakefriends-noheader.csv");
        JavaPairRDD<Integer, Integer> rdd = lines.mapToPair(new ParseLine());
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> totalsByAge = rdd.mapValues(
                (Function<Integer, Tuple2<Integer, Integer>>) x -> new Tuple2(x, 1)
        ).reduceByKey((v1, v2) -> new Tuple2(v1._1() + v2._1(), v1._2() + v2._2()));
        JavaPairRDD<Integer, Integer> averagesByAge = totalsByAge.mapValues(
                (Function<Tuple2<Integer, Integer>, Integer>) v -> v._1() / v._2()
        );
        List<Tuple2<Integer, Integer>> results = averagesByAge.collect();
        results.stream().sorted(Comparator.comparingInt(Tuple2::_1)).collect(Collectors.toList()).
                forEach(x -> System.out.println("(" + x._1() + ", " + x._2() + ")"));
    }
}
