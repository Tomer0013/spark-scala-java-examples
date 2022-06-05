package spark.java;

import org.apache.spark.api.java.*;
import org.apache.log4j.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;


public class MostPopularSuperhero {
    public static class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) {
            return x._1() - y._1();
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "MostPopularSuperhero");

        JavaPairRDD<Integer, String> namesRdd = sc.textFile("./data/marvel-names.txt")
                .map((Function<String, Optional<Tuple2<Integer, String>>>) line -> {
                    String[] fields = line.split("\"");
                    if (fields.length > 1) {
                        return Optional.of(new Tuple2<>(Integer.parseInt(fields[0].trim()), fields[1]));
                    } else {
                        return Optional.empty();
                    }
                })
                .filter(x -> x.isPresent())
                .map(x -> x.get())
                .mapToPair(x -> new Tuple2<>(x._1(), x._2()));

        JavaPairRDD<Integer, Integer> pairings = sc.textFile("./data/marvel-graph.txt")
                .mapToPair((PairFunction<String, Integer, Integer>) line -> {
                    String[] fields = line.split("\\W+");
                    return new Tuple2(Integer.parseInt(fields[0]), fields.length - 1);
                });

        JavaPairRDD<Integer, Integer> totalFriendsByCharacter = pairings.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<Integer, Integer> flipped = totalFriendsByCharacter.mapToPair(x -> new Tuple2<>(x._2(), x._1()));
        Tuple2<Integer, Integer> mostPopular = flipped.max(new TupleComparator());
        String mostPopularName = namesRdd.lookup(mostPopular._2()).get(0);

        System.out.println(mostPopularName + " is the most popular superhero with " + mostPopular._1() + " co-appearances.");
    }

}
