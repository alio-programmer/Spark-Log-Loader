package org.example.Extractor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Extractrawlogs {
    private final SparkSession spark;

    public Extractrawlogs(SparkSession spark){
        this.spark = spark;
    }

    public Dataset<Row> logLoader(String filepath){
        Dataset<Row> df = spark.read().text(filepath).withColumnRenamed("value", "logs");
        return df;
    }
}
