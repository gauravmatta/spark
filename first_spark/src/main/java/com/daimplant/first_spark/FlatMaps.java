package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMaps {

    public static void main(String[] args) {
        List<String> inputData = getStrings();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf()
                .setAppName("FlatMap Spark Application")
                .setMaster("local[*]");
        try(JavaSparkContext sparkContext = new JavaSparkContext(conf)){
            JavaRDD<String> sentences = sparkContext.parallelize(inputData);
            sentences
                    .map(sentence -> {
                        System.out.println(sentence);
                        return sentence;
                    }) // Print each sentence
            .flatMap(value -> {
                String[] words = value.split(" ");
                List<String> wordList = new ArrayList<>(Arrays.asList(words));
                return wordList.iterator();
            })
            .filter(word -> word.length() > 3) // Filter words longer than 3 characters
            .foreach(word -> System.out.println("Word: " + word));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
