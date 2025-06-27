package org.example.Parser;

import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class CreateRegexMap {
    private SparkSession spark;
    public CreateRegexMap(SparkSession spark){
        this.spark = spark;
    }

    public Map<String, Pattern> createTemplateRegexMap(Map<String, String> tempmap) {
        Map<String, Pattern> regexmap = new HashMap<>();

        tempmap.forEach((eventId, template) -> {
            // Split the template by <*> and build regex
            String[] parts = template.split("<\\*>");
            StringBuilder regexBuilder = new StringBuilder();

            for (int i = 0; i < parts.length; i++) {
                regexBuilder.append(Pattern.quote(parts[i]));
                if (i != parts.length - 1) {
                    regexBuilder.append("(.+?)");
                }
            }

            Pattern regex = Pattern.compile(regexBuilder.toString());
            regexmap.put(eventId, regex);
        });

        return regexmap;
    }

}
