package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import java.io.Serializable;
import static org.apache.spark.sql.functions.col;


public class DataFramesDataset {

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
        SparkSession spark = SparkSession.builder().appName("DataFramesDataset").master("local[*]").getOrCreate();
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> people = spark.read().option("header", true).option("inferSchema", true).csv("./data/fakefriends.csv").as(personEncoder);
        System.out.println("Here is our inferred schema:");
        people.printSchema();

        System.out.println("Let's select the name column:");
        people.select("name").show();

        System.out.println("Filter out anyone over 21:");
        people.filter(col("age").lt(21)).show();

        System.out.println("Group by age:");
        people.groupBy("age").count().show();

        System.out.println("Make everyone 10 years older:");
        people.select(col("name"), col("age").$plus(10)).show();

        spark.stop();
    }
}
