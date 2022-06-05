package spark.java;

import org.apache.log4j.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.io.Serializable;
import java.util.Arrays;


public class WordCountBetterSortedDataset {

    public static class Book implements Serializable{
        private String value;
        public String getValue() {return value;}
        public void setValue(String v) {this.value = v;}
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("WordCountBetterSortedDataset").master("local[*]").getOrCreate();
        Encoder<Book> bookEncoder = Encoders.bean(Book.class);
        Dataset<Book> input = spark.read().text("./data/book.txt").as(bookEncoder);
        Dataset<Row> words = input.select(explode(split(col("value"), "\\W+")).alias("word")).filter(col("word").notEqual(""));
        Dataset<Row> lowercaseWords = words.select(lower(col("word")).alias("word"));
        Dataset<Row> wordCounts = lowercaseWords.groupBy("word").count();
        Dataset<Row> wordCountsSorted = wordCounts.sort("count");
        wordCountsSorted.show((int) wordCountsSorted.count());

        // Another way with both Datasets and RDDs:
        JavaRDD<String> booksRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile("./data/book.txt");
        JavaRDD<Book> wordsRDD = booksRDD.flatMap((FlatMapFunction<String, String>) x -> Arrays.stream(x.split("\\W+")).iterator())
                .map((Function<String, Book>) s -> {
                    Book book = new Book();
                    book.setValue(s);
                    return book;
                });
        Dataset<Row> wordsDS = spark.createDataFrame(wordsRDD, Book.class);
        Dataset<Row> lowercaseWordsDS = wordsDS.select(lower(col("value")).alias("word"));
        Dataset<Row> wordCountsDS = lowercaseWordsDS.groupBy("word").count();
        Dataset<Row> wordsCountsDSSorted = wordCountsDS.sort("count");
        wordsCountsDSSorted.show((int) wordsCountsDSSorted.count());

        spark.stop();
    }

}
