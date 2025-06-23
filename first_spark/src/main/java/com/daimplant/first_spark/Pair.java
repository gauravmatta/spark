package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Pair {
    public static void main(String[] args) {
        List<String> inputData = getStrings();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Pair Spark Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logRdd = sc.parallelize(inputData);
        JavaPairRDD<String, String> pairRdd = logRdd.mapToPair(value -> {
            String[] columns = value.split(": ", 2);
            if (columns.length == 2) {
                return new Tuple2<>(columns[0], columns[1]);
            } else {
                return new Tuple2<>("UNKNOWN", value);
            }
        });
        // Print the key-value pairs
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = pairRdd.groupByKey();
        stringIterableJavaPairRDD.foreach(tuple -> {
            String key = tuple._1;
            Iterable<String> values = tuple._2;
            System.out.println("Key: " + key);
            System.out.println("Values:");
            for (String value : values) {
                System.out.println("  " + value);
            }
        });
        // Reduce the key-value pairs
        pairRdd.reduceByKey((value1, value2) -> value1 + ", " + value2)
                .foreach(tuple -> System.out.println("Reduced Key: " + tuple._1 + ", Values: " + tuple._2));

        // Count the occurrences of each key
        logRdd.mapToPair(value -> {
                    String[] columns = value.split(": ", 2);
                    String level = columns[0];
                    return new Tuple2<>(level, 1L);
                }).reduceByKey(Long::sum)
                .foreach(tuple -> System.out.println("Key: " + tuple._1 + ", Count: " + tuple._2));
        sc.close();
    }

    private static @NotNull List<String> getStrings() {
        List<String> inputData = new ArrayList<>();
        inputData.add("INFO: Running Spark version 4.0.0");
        inputData.add("INFO: OS info Linux, 6.14.9-300.fc42.x86_64, amd64");
        inputData.add("INFO: Java version 21.0.7");
        inputData.add("WARN: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable");
        inputData.add("INFO: ==============================================================");
        inputData.add("INFO: No custom resources configured for spark.driver.");
        inputData.add("INFO: Submitted application: First Spark Application");
        inputData.add("WARN: OutputCommitCoordinator stopped!");
        inputData.add("Error: Successfully stopped SparkContext");
        inputData.add("FATAL: Shutdown hook called");
        inputData.add("Error: Deleting directory /tmp/spark-3582b043-23bb-4775-b057-a8261a2cbbfe");
        return inputData;
    }
}
