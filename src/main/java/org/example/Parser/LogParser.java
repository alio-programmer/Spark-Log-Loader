package org.example.Parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Column;

public class LogParser {
    private final SparkSession spark;

    public LogParser(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> parseLogs(Dataset<Row> logs) {
        Column splitExpr = split(col("logs"), " ", 6);
        Dataset<Row> df2 = logs
                .withColumn("Date",     splitExpr.getItem(0))
                .withColumn("Time",     splitExpr.getItem(1))
                .withColumn("PID",      splitExpr.getItem(2))
                .withColumn("Level",    splitExpr.getItem(3))
                .withColumn("Component",splitExpr.getItem(4))
                .withColumn("Content",  splitExpr.getItem(5))
                .drop("logs");

        df2.show(5, false);
        return df2;
    }

}

