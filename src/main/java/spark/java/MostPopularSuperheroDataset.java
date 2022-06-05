package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MostPopularSuperheroDataset {

    public static class SuperHeroNames implements Serializable {
        private int id;

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

        private String name;
    }

    public static class Superhero implements Serializable {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MostPopularSuperheroDataset").master("local[*]").getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType superheroNamesSchema = DataTypes.createStructType(fields);
        Encoder<SuperHeroNames> superHeroNamesEncoder = Encoders.bean(SuperHeroNames.class);
        Dataset<SuperHeroNames> names = spark.read().option("sep", " ")
                .schema(superheroNamesSchema)
                .csv("./data/marvel-names.txt")
                .as(superHeroNamesEncoder);

        Encoder<Superhero> superheroEncoder = Encoders.bean(Superhero.class);
        Dataset<Superhero> lines = spark.read().text("./data/marvel-graph.txt").as(superheroEncoder);

        Dataset<Row> connections = lines.withColumn("id", split(col("value"), " ").getItem(0))
                .withColumn("connections", size(split(col("value"), " ")).minus(1))
                .groupBy("id").agg(sum("connections").alias("connections"));

        Row mostPopular = connections.sort(col("connections").desc()).first();
        Row mostPopularName = names.filter(col("id").equalTo(mostPopular.get(0))).select("name").first();
        System.out.println(mostPopularName.get(0) + " is the most popular superhero with " + mostPopular.get(1) + " co-appearances.");

        spark.stop();
    }

}
