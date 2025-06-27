package org.example.Parser;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.regex.Pattern;

public class BroadcastRegexMap {
    private SparkSession spark;
    public BroadcastRegexMap(SparkSession spark){
        this.spark = spark;
    }
    public Broadcast<Map<String, Pattern>> broadcastRegexMap(Map<String, Pattern> regexmap){
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        return sc.broadcast(regexmap);
    }
}
