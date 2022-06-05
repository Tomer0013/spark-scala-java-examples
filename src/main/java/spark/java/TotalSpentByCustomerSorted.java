package spark.java;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.List;

public class TotalSpentByCustomerSorted {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "TotalSpentByCustomerSorted");
        JavaRDD<String> lines = sc.textFile("./data/customer-orders.csv");
        JavaPairRDD<Integer, Float> customerSpent = lines.mapToPair((PairFunction<String, Integer, Float>) line -> {
            String[] fields = line.split(",");
            int customerID = Integer.parseInt(fields[0]);
            float amount = Float.parseFloat(fields[2]);
            return new Tuple2<>(customerID, amount);
        }).reduceByKey(Float::sum);
        JavaPairRDD<Float, Integer> customerSpentSorted = customerSpent.mapToPair(s -> new Tuple2<>(s._2(), s._1())).sortByKey(false, 1);
        List<Tuple2<Float, Integer>> results = customerSpentSorted.collect();
        results.forEach(x -> System.out.println("Customer " + x._2() + ": " + String.format("%.2f", x._1())));
    }

}
