package org.example.Parser;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;
import java.util.regex.Pattern;

public class TemplateMatcher {
    private SparkSession spark;

    public TemplateMatcher(SparkSession spark){
        this.spark = spark;
    }

    public Dataset<Row> matchTemplate(Dataset<Row> logdf, Broadcast<Map<String, Pattern>> regexmap){
        spark.udf().register("matchEventId", (UDF1<String, String>) content -> {
            for (Map.Entry<String, Pattern> entry : regexmap.value().entrySet()) {
                if (entry.getValue().matcher(content).find()) {
                    return entry.getKey();
                }
            }
            return "Unknown";
        }, DataTypes.StringType);
        return logdf.withColumn("EventId", functions.callUDF("matchEventId", logdf.col("Content")));
    }

}
