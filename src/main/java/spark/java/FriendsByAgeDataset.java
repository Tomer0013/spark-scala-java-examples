package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import java.io.Serializable;
import static org.apache.spark.sql.functions.*;

public class FriendsByAgeDataset {

    public static class Person implements Serializable {
        private int id;
        private String name;
        private int age;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getFriends() {
            return Friends;
        }

        public void setFriends(int friends) {
            Friends = friends;
        }

        private int Friends;

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("FriendsByAgeDataset").master("local[*]").getOrCreate();
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> ds = spark.read().option("header", true).option("inferSchema", true).csv("./data/fakefriends.csv").as(personEncoder);

        // Select only age and numFriends columns
        Dataset<Row> friendsByAge = ds.select("age", "friends");

        // From friendsByAge we group by "age" and then compute average
        friendsByAge.groupBy("age").avg("friends").show();

        // Sorted:
        friendsByAge.groupBy("age").avg("friends").sort("age").show();

        // Formatted more nicely with a custom column name:
        friendsByAge.groupBy("age").agg(round(avg("friends"), 2)).alias("friends_avg").sort("age").show();

        spark.stop();
   }
}
