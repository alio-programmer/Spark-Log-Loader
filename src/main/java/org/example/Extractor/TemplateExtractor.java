package org.example.Extractor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class TemplateExtractor {
    private final SparkSession spark;

    public TemplateExtractor(SparkSession spark){
        this.spark = spark;
    }

    public Map<String, String> templateLoader(String templatepath){
        //Collect template in spark dataset
        Dataset<Row> df = spark.read().option("header", true).option("inferSchema", true).csv(templatepath);

        //load template in java map for processing during parsing
        Map<String, String> mappedTemplate = new HashMap<>();
        df.collectAsList().forEach(row -> {
            String eventId = row.getAs("EventId");
            String template = row.getAs("EventTemplate");
            mappedTemplate.put(eventId.trim(), template.trim());
        });

        return mappedTemplate;
    }
}
