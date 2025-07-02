package org.example;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Extractor.Extractrawlogs;
import org.example.Extractor.TemplateExtractor;
import org.example.Parser.BroadcastRegexMap;
import org.example.Parser.CreateRegexMap;
import org.example.Parser.LogParser;
import org.example.Parser.TemplateMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String filepath = "C:\\Users\\Gaurav\\Downloads\\HDFS.log";
        String templatePath = "C:\\Users\\Gaurav\\Downloads\\HDFS_2k.log_templates.csv";

        String os = System.getProperty("os.name").toLowerCase();

        SparkSession spark = SparkSession.builder()
                .appName("logloader")
                .master("local[*]")
                .getOrCreate();

        logger.info("Spark session started");

        // STEP 1: Loading raw logs
        Extractrawlogs logs = new Extractrawlogs(spark);
        Dataset<Row> rawDF = logs.logLoader(filepath);

        // STEP 2: Extracting log templates
        TemplateExtractor templates = new TemplateExtractor(spark);
        Map<String, String> mappedtemplates = templates.templateLoader(templatePath);
        logger.info("Loaded {} templates", mappedtemplates.size());
        mappedtemplates.forEach((k, v) -> logger.debug("Template [{}] -> {}", k, v));

        // STEP 3: Parsing logs into structured format
        LogParser logparse = new LogParser(spark);
        Dataset<Row> parseDF = logparse.parseLogs(rawDF);
        logger.info("Parsed logs into structured format");

        // STEP 4: Creating regex map from templates
        CreateRegexMap regexmap = new CreateRegexMap(spark);
        Map<String, Pattern> regmap = regexmap.createTemplateRegexMap(mappedtemplates);
        logger.info("Created regex map for templates");

        // STEP 5: Broadcasting regex map
        BroadcastRegexMap broadcastregex = new BroadcastRegexMap(spark);
        Broadcast<Map<String, Pattern>> broadval = broadcastregex.broadcastRegexMap(regmap);
        logger.info("Broadcasted regex map to executors");

        // STEP 6: Matching templates to parsed logs
        TemplateMatcher tempmatch = new TemplateMatcher(spark);

        Dataset<Row> parsedDF = tempmatch.matchTemplate(parseDF, broadval).cache();

        System.out.println("Number of logs processed:"+parsedDF.count()); //action 1 to materialize full dataset in cache

        parsedDF.show(5, false);   //action 2
        logger.info("Template matching completed");

        // STEP 7: Saving output (for different operating systems)
        if (os.contains("win")) {
            System.out.println("Windows OS detected. HADOOP_HOME set.");

            // Set Windows local output path
            String outputPath = "C:\\Users\\Gaurav\\Downloads\\Main_output_parsed_logs";
            parsedDF.write()      //action 3
                    .option("header", true)
                    .mode("overwrite")
                    .csv(outputPath);

            System.out.println("Output written to: " + outputPath);
        } else {
            System.out.println("Non-Windows OS detected. Skipping HADOOP_HOME setup.");

            // Set Linux/macOS local output path (e.g., to /tmp/output_parsed_logs)
            String outputPath = "/home/gaurav/output-parsed-logs";
            parsedDF.write()     //action 3
                    .option("header", true)
                    .mode("overwrite")
                    .csv(outputPath);

            System.out.println("Output written to: " + outputPath);
        }


        logger.info("Parsed logs saved successfully");

        // Final log and shutdown
        logger.info("Pipeline completed successfully");
        spark.stop();
        logger.info("Spark session stopped");
    }
}
