package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;


public class MovieSimilaritiesDataset {

    public static class Movies implements Serializable {
        private int userID;
        private int movieID;
        private int rating;
        private long timestamp;

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
    }

    public static class MoviesNames implements Serializable {
        private int movieID;
        private String movieTitle;

        public int getMovieID() {
            return movieID;
        }

        public void setMovieID(int movieID) {
            this.movieID = movieID;
        }

        public String getMovieTitle() {
            return movieTitle;
        }

        public void setMovieTitle(String movieTitle) {
            this.movieTitle = movieTitle;
        }
    }

    public static class MoviePairs implements Serializable {
        private int movie1;
        private int movie2;
        private int rating1;
        private int rating2;

        public int getMovie1() {
            return movie1;
        }

        public void setMovie1(int movie1) {
            this.movie1 = movie1;
        }

        public int getMovie2() {
            return movie2;
        }

        public void setMovie2(int movie2) {
            this.movie2 = movie2;
        }

        public int getRating1() {
            return rating1;
        }

        public void setRating1(int rating1) {
            this.rating1 = rating1;
        }

        public int getRating2() {
            return rating2;
        }

        public void setRating2(int rating2) {
            this.rating2 = rating2;
        }
    }

    public static class MoviePairsSimilarity {
        private int movie1;
        private int movie2;
        private double score;
        private long numPairs;

        public int getMovie1() {
            return movie1;
        }

        public void setMovie1(int movie1) {
            this.movie1 = movie1;
        }

        public int getMovie2() {
            return movie2;
        }

        public void setMovie2(int movie2) {
            this.movie2 = movie2;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        public long getNumPairs() {
            return numPairs;
        }

        public void setNumPairs(long numPairs) {
            this.numPairs = numPairs;
        }
    }

    public static Dataset<MoviePairsSimilarity> computeCosineSimilarity(Dataset<MoviePairs> data) {
        Dataset<Row> pairScores = data
                .withColumn("xx", col("rating1").multiply(col("rating1")))
                .withColumn("yy", col("rating2").multiply(col("rating2")))
                .withColumn("xy", col("rating1").multiply(col("rating2")));
        Dataset<Row> calculateSimilarity = pairScores.groupBy("movie1", "movie2")
                .agg(
                        sum(col("xy")).alias("numerator"),
                        (sqrt(sum(col("xx"))).multiply(sqrt(sum(col("yy"))))).alias("denominator"),
                        count(col("xy")).alias("numPairs")
                );
        Encoder<MoviePairsSimilarity> moviePairsSimilarityEncoder = Encoders.bean(MoviePairsSimilarity.class);
        Dataset<MoviePairsSimilarity> result = calculateSimilarity
                .withColumn("score", when(col("denominator").notEqual(0), col("numerator").divide(col("denominator")))
                        .otherwise(null)).select("movie1", "movie2", "score", "numPairs").as(moviePairsSimilarityEncoder);
        return result;
    }

    public static String getMovieName(Dataset<MoviesNames> movieNames, int movieID) {
        String result = movieNames.filter(col("movieID").equalTo(movieID))
                .select("movieTitle").collectAsList().get(0).getString(0);
        return result;
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MovieSimilaritiesDataset").master("local[*]").getOrCreate();

        StructType movieNamesSchema = DataTypes.createStructType(
                new ArrayList<>(Arrays.asList(
                        DataTypes.createStructField("movieID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("movieTitle", DataTypes.StringType, true)
                )));

        StructType moviesSchema = DataTypes.createStructType(
                new ArrayList<>(Arrays.asList(
                        DataTypes.createStructField("userID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("movieID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("rating", DataTypes.IntegerType, true),
                        DataTypes.createStructField("timestamp", DataTypes.LongType, true)
                )));

        System.out.println("\nLoading movie names...");
        Encoder<MoviesNames> movieNamesEncoder = Encoders.bean(MoviesNames.class);
        Dataset<MoviesNames> movieNames = spark.read().option("sep", "|").option("charset", "ISO-8859-1")
                .schema(movieNamesSchema).csv("./data/ml-100k/u.item").as(movieNamesEncoder);

        Encoder<Movies> moviesEncoder = Encoders.bean(Movies.class);
        Dataset<Movies> movies = spark.read().option("sep", "\t").schema(moviesSchema)
                .csv("./data/ml-100k/u.data").as(moviesEncoder);

        Dataset<Row> ratings = movies.select("userID", "movieID", "rating");
        Encoder<MoviePairs> moviePairsEncoder = Encoders.bean(MoviePairs.class);
        Dataset<MoviePairs> moviePairs = ratings.as("ratings1")
                .join(ratings.as("ratings2"),
                        col("ratings1.userID").equalTo(col("ratings2.userID"))
                        .and(col("ratings1.movieID").lt(col("ratings2.movieID"))))
                .select(col("ratings1.movieID").alias("movie1"),
                        col("ratings2.movieID").alias("movie2"),
                        col("ratings1.rating").alias("rating1"),
                        col("ratings2.rating").alias("rating2")
                ).as(moviePairsEncoder);

        Dataset<MoviePairsSimilarity> moviePairsSimilarities = computeCosineSimilarity(moviePairs).cache();

        if (args.length > 0) {
            float scoreThreshold = 0.97f;
            float coOccurrenceThreshold = 50.0f;
            int movieID = Integer.parseInt(args[0]);
            MoviePairsSimilarity[] results = (MoviePairsSimilarity[]) moviePairsSimilarities
                    .filter((col("movie1").equalTo(movieID).or(col("movie2").equalTo(movieID)))
                            .and(col("score").gt(scoreThreshold))
                            .and(col("numPairs").gt(coOccurrenceThreshold)))
                    .sort(col("score").desc()).take(10);

            System.out.println("\nTop 10 similar movies for " + getMovieName(movieNames, movieID));
            for(MoviePairsSimilarity result: results) {
                int similarMovieID = result.getMovie1();
                if (similarMovieID == movieID){
                    similarMovieID = result.getMovie2();
                }
                System.out.println(getMovieName(movieNames, similarMovieID) + "\tscore: " + result.getScore() + "\tstrength: " + result.getNumPairs());
            }

        }
    }
}

