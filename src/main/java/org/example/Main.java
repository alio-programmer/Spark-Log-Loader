package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Extractor.Extractrawlogs;
import org.example.Extractor.TemplateExtractor;

import java.util.Map;

public class Main {
    public static void main(String[] args) {

        String filepath = "C:\\Users\\Gaurav\\Downloads\\HDFS_2k.log";
        String templatePath = "C:\\Users\\Gaurav\\Downloads\\HDFS_2k.log_templates.csv";
        SparkSession spark = SparkSession.builder().appName("logloader").master("local[*]").getOrCreate();

        //loading logs
        Extractrawlogs logs = new Extractrawlogs(spark);
        Dataset<Row> logDF = logs.logLoader(filepath);
        logDF.show(10);
        System.out.println("Extracted total:"+logDF.count()+" number of rows");
        //Working Completed ✅

        //extracting log Templates
        TemplateExtractor templates = new TemplateExtractor(spark);
        Map<String, String> mappedtemplates = templates.templateLoader(templatePath);
        mappedtemplates.forEach((K, V) -> {
            System.out.println(K + "->" + V);
        });
        //Working Completed ✅

        //parsing logs and matching them to their templates

        //saving the output as a CSV file

        spark.stop();

    }
}