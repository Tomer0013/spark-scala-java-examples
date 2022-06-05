package spark.java;

import org.apache.log4j.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;


class BFSData implements Serializable {
    final private Integer[] connections;
    final private int distance;
    final private String color;

    public BFSData(Integer[] connections, int distance, String color) {
        this.connections = connections;
        this.distance = distance;
        this.color = color;
    }

    public Integer[] getConnections() {
        return connections;
    }

    public int getDistance() {
        return distance;
    }

    public String getColor() {
        return color;
    }
}

class BFSNode implements Serializable {
    final private int id;
    final private BFSData data;

    public BFSNode(int id, Integer[] data, int distance, String color) {
        this.id = id;
        this.data = new BFSData(data, distance, color);
    }

    public int getId() {
        return id;
    }

    public BFSData getData() {
        return data;
    }
}

class BFSMap implements FlatMapFunction<BFSNode, BFSNode> {
    public Iterator<BFSNode> call(BFSNode node) {
        int characterID = node.getId();
        BFSData data = node.getData();
        Integer[] connections = data.getConnections();
        int distance = data.getDistance();
        String color = data.getColor();
        ArrayList<BFSNode> results = new ArrayList<>();

        if (color.equals("GRAY")) {
            for (int connection: connections) {
                int newDistance = distance + 1;
                String newColor = "GRAY";
                if (DegreesOfSeparation.targetCharacterID == connection) {
                    if (DegreesOfSeparation.hitCounter.isPresent()) {
                        DegreesOfSeparation.hitCounter.get().add(1);
                    }
                }
                BFSNode newEntry = new BFSNode(connection, new Integer[0], newDistance, newColor);
                results.add(newEntry);
            }
            color = "BLACK";
        }
        BFSNode thisEntry = new BFSNode(characterID, connections, distance, color);
        results.add(thisEntry);

        return results.iterator();
    }
}

class BFSReduce implements Function2<BFSData, BFSData, BFSData> {
    public BFSData call(BFSData data1, BFSData data2) {
        Integer[] edges1 = data1.getConnections();
        Integer[] edges2 = data2.getConnections();
        int distance1 = data1.getDistance();
        int distance2 = data2.getDistance();
        String color1 = data1.getColor();
        String color2 = data2.getColor();

        int distance = 9999;
        String color = "WHITE";
        ArrayList<Integer> edges = new ArrayList<>();

        if (edges1.length > 0){
            edges.addAll(Arrays.asList(edges1));
        }
        if (edges2.length > 0){
            edges.addAll(Arrays.asList(edges2));
        }
        if (distance1 < distance){
            distance = distance1;
        }
        if (distance2 < distance){
            distance = distance2;
        }

        if (color1.equals("WHITE") && (color2.equals("GRAY") || color2.equals("BLACK"))) {
            color = color2;
        }
        if (color1.equals("GRAY") && color2.equals("BLACK")) {
            color = color2;
        }
        if (color2.equals("WHITE") && (color1.equals("GRAY") || color1.equals("BLACK"))) {
            color = color1;
        }
        if (color2.equals("GRAY") && color1.equals("BLACK")) {
            color = color1;
        }
        if (color1.equals("GRAY") && color2.equals("GRAY")) {
            color = color1;
        }
        if (color1.equals("BLACK") && color2.equals("BLACK")) {
            color = color1;
        }
        return new BFSData(edges.toArray(new Integer[edges.size()]), distance, color);
    }
}

class ConvertToBFS implements Function<String, BFSNode> {
    public BFSNode call(String line) {
        String[] fields = line.split("\\s+");
        int heroID = Integer.parseInt(fields[0]);
        ArrayList<Integer> connections = new ArrayList<>();
        Arrays.stream(fields).skip(1).forEach(x -> connections.add(Integer.parseInt(x)));
        String color = "WHITE";
        int distance = 9999;
        if (heroID == DegreesOfSeparation.startCharacterID) {
            color = "GRAY";
            distance = 0;
        }
        return new BFSNode(heroID, connections.toArray(new Integer[connections.size()]), distance, color);
    }
}

public class DegreesOfSeparation {
    static int startCharacterID = 5306;
    static int targetCharacterID = 14;
    static Optional<LongAccumulator> hitCounter = Optional.empty();

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext("local[*]", "DegreesOfSeparation");
        DegreesOfSeparation.hitCounter = Optional.of(JavaSparkContext.toSparkContext(sc).longAccumulator());
        JavaRDD<BFSNode> iterationRdd = sc.textFile("./data/marvel-graph.txt").map(new ConvertToBFS());
        for (int iteration = 1; iteration <= 10; iteration++) {
            System.out.println("Running BFS Iteration# " + iteration);
            JavaRDD<BFSNode> mapped = iterationRdd.flatMap(new BFSMap());
            System.out.println("Processing " + mapped.count() + " values.");
            if (DegreesOfSeparation.hitCounter.isPresent()){
                long hitCount = hitCounter.get().value();
                if (hitCount > 0){
                    System.out.println("Hit the target character! From " + hitCount + " different direction(s).");
                    return;
                }
            }
            iterationRdd = mapped
                    .mapToPair(x -> new Tuple2<>(x.getId(), x.getData()))
                    .reduceByKey(new BFSReduce())
                    .map(x -> new BFSNode(x._1(), x._2().getConnections(), x._2().getDistance(), x._2().getColor()));
        }

    }

}
