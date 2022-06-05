package spark.java;

import org.apache.spark.sql.*;
import org.apache.log4j.*;
import java.io.Serializable;
import java.util.List;


public class SparkSQLDataset {

    public static class Person implements Serializable {
        private int id;
        private String name;
        private int age;
        private int friends;

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
            return friends;
        }

        public void setFriends(int friends) {
            this.friends = friends;
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("SparkSQLDataset").master("local[*]").getOrCreate();
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> schemaPeople = spark.read().option("header", "true").option("inferSchema", "true").csv("./data/fakefriends.csv").as(personEncoder);
        schemaPeople.printSchema();
        schemaPeople.createOrReplaceTempView("people");
        Dataset<Row> teenagers = spark.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 19");
        List<Row> results = teenagers.collectAsList();
        results.forEach(System.out::println);
        spark.stop();
    }
}
