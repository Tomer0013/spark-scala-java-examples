package spark.java;

import org.apache.log4j.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class PopularMoviesNicerDataset {

    public static class Movies implements Serializable {
        private int userID;
        private int movieID;

        public int getUserID() {
            return userID;
        }

        public void setUserID(int userID) {
            this.userID = userID;
        }

        public int getMovieID() {
            return movieID;
        }

        public void setMovieID(int movieID) {
            this.movieID = movieID;
        }

        public int getRating() {
            return rating;
        }

        public void setRating(int rating) {
            this.rating = rating;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        private int rating;
        private long timestamp;

    }

    public static HashMap<Integer, String> loadMovieNames() {
        HashMap<Integer, String> movieToTitleMap = new HashMap<>();
        try {
            BufferedReader csvReader = new BufferedReader(new InputStreamReader(new FileInputStream("./data/ml-100k/u.item"), "ISO_8859_1"));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] fields = row.split("[|]+");
                movieToTitleMap.put(Integer.parseInt(fields[0]), fields[1]);
            }
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return movieToTitleMap;
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("PopularMoviesNicerDataset").master("local[*]").getOrCreate();
        JavaSparkContext javaSc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Broadcast<HashMap<Integer, String>> nameDict = javaSc.broadcast(loadMovieNames());

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("userID", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("movieID", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("rating", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(fields);

        Encoder<Movies> moviesEncoder = Encoders.bean(Movies.class);
        Dataset<Movies> movies = spark.read().option("sep", "\t").schema(schema).csv("./data/ml-100k/u.data").as(moviesEncoder);
        Dataset<Row> movieCounts = movies.groupBy("movieID").count();

        UserDefinedFunction lookupName= udf((Integer movieID) -> nameDict.value().get(movieID), DataTypes.StringType);
        spark.udf().register("lookupNameUDF", lookupName);

        Dataset<Row> moviesWithNames = movieCounts
                .withColumn("movieTitle", functions.callUDF("lookupNameUDF", col("movieID")))
                        .sort("count");

        moviesWithNames.show((int) moviesWithNames.count(), false);

        spark.stop();
    }
}
