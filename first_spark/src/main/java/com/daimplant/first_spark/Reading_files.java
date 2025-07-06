package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Reading_files {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf()
                .setAppName("Reading Files Spark Application")
                .setMaster("local[*]");
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            // Read a text file from the local filesystem
            JavaRDD<String> lines = sparkContext.textFile("src/main/resources/subtitles/300.txt");

            lines.map(line ->{
                System.out.println("Line: " + line);
                return line;
            }).flatMap(value -> Arrays.asList(value.split(" ")).iterator())
            .foreach(word -> System.out.println("Word: " + word));

            // Count the number of lines in the file
            long lineCount = lines.count();
            System.out.println("Total number of lines: " + lineCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
