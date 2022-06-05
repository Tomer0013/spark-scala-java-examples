package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TotalSpentByCustomerSortedDataset {

    public static class Order implements Serializable {
        public int getCustomerID() {
            return customerID;
        }

        public void setCustomerID(int customerID) {
            this.customerID = customerID;
        }

        public int getItemID() {
            return itemID;
        }

        public void setItemID(int itemID) {
            this.itemID = itemID;
        }

        public float getAmount() {
            return amount;
        }

        public void setAmount(float amount) {
            this.amount = amount;
        }

        private int customerID;
        private int itemID;
        private float amount;
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("TotalSpentByCustomerSortedDataset").master("local[*]").getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("customerID", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("itemID", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("amount", DataTypes.FloatType, true));
        StructType schema = DataTypes.createStructType(fields);

        Encoder<Order> orderEncoder = Encoders.bean(Order.class);
        Dataset<Order> ds = spark.read().schema(schema).csv("./data/customer-orders.csv").as(orderEncoder);
        Dataset<Row> customerSpent = ds.groupBy("customerID")
                .agg(round(sum("amount"), 2).alias("totalSpent"))
                .sort(col("totalSpent").desc());

        List<Row> results = customerSpent.collectAsList();
        for(Row res: results) {
            System.out.println("Customer " + res.get(0) + ": " + res.get(1));
        }
        spark.stop();
    }

}
