package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.types.*;
import java.io.Serializable;
import java.util.*;
import scala.collection.mutable.ArraySeq;
import static org.apache.spark.sql.functions.*;


public class MovieRecommendationsALSDataset {

    public static class MovieNames implements Serializable {
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

    public static class Rating implements Serializable {
        private int userID;
        private int movieID;
        private int rating;

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
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MovieRecommendationsALSDataset").master("local[*]").getOrCreate();

        System.out.println("Loading movie names...");
        StructType movieNamesSchema = DataTypes.createStructType(
                new ArrayList<>(Arrays.asList(
                    DataTypes.createStructField("movieID", DataTypes.IntegerType, true),
                    DataTypes.createStructField("movieTitle", DataTypes.StringType, true)
                ))
        );
        Encoder<MovieNames> movieNamesEncoder = Encoders.bean(MovieNames.class);
        List<MovieNames> names = spark.read().option("sep", "|").option("charset", "ISO-8859-1")
                .schema(movieNamesSchema).csv("./data/ml-100k/u.item").as(movieNamesEncoder).collectAsList();
        HashMap<Integer, String> movieIdToNameMap = new HashMap<>();
        names.forEach(x -> movieIdToNameMap.put(x.getMovieID(), x.getMovieTitle()));

        System.out.println("Loading ratings...");
        StructType ratingSchema = DataTypes.createStructType(
                new ArrayList<>(Arrays.asList(
                        DataTypes.createStructField("userID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("movieID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("rating", DataTypes.IntegerType, true)
                ))
        );
        Encoder<Rating> ratingEncoder = Encoders.bean(Rating.class);
        Dataset<Rating> ratings = spark.read().option("sep", "\t").schema(ratingSchema)
                        .csv("./data/ml-100k/u.data").as(ratingEncoder);

        System.out.println("Training recommendations model...");
        ALS als = new ALS().setMaxIter(10).setAlpha(40).setRank(100).setRegParam(0.05)
                .setUserCol("userID").setItemCol("movieID").setRatingCol("rating");
        ALSModel model = als.fit(ratings);

        int userID = Integer.parseInt(args[0]);
        Dataset<Row> users = ratings.filter(col(als.getUserCol()).equalTo(userID)).distinct().select(als.getUserCol());
        List<Row> recommendations = model.recommendForUserSubset(users, 10).collectAsList();

        for(Row recommendations_for_user: recommendations){
            System.out.println("Top 10 recommendations for user ID " + recommendations_for_user.getAs("userID"));
            ArraySeq<Row> recs = recommendations_for_user.getAs("recommendations");
            for(int i=0; i < recs.length(); i++) {
                Row rec = recs.apply(i);
                System.out.println(movieIdToNameMap.get((int) rec.getAs("movieID")) + ", " + rec.getAs("rating"));
            }
        }
        spark.stop();

    }

}
