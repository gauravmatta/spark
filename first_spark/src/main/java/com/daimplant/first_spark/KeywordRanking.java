package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class KeywordRanking {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf()
                .setAppName("Reading Files Spark Application")
                .setMaster("local[*]");
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            // Read a text file from the local filesystem
            JavaRDD<String> initialRdd = sparkContext.textFile("src/main/resources/subtitles/300.txt");
            List<String> results = initialRdd.take(2000);
            results.forEach(System.out::println);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
