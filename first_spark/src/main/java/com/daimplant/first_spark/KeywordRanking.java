package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class KeywordRanking {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\winutils");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Reading Files Spark Application").setMaster("local[*]");
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            // Read a text file from the local filesystem
            JavaRDD<String> initialRdd = sparkContext.textFile("src/main/resources/subtitles/300.txt");
            JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence
                    .replaceAll("[^a-zA-Z\\s]", "")
                    .toLowerCase().trim());
            JavaRDD<String> removedEmptyLinesRdd = lettersOnlyRdd.filter(line -> !line.isEmpty());
            JavaRDD<String> justWordsRdd = removedEmptyLinesRdd.flatMap(line -> List.of(line.split("\\s+")).iterator());
            JavaRDD<String> justInterstingWordsRdd = justWordsRdd.filter(Util::isNotBoring);
            JavaPairRDD<String, Long> pairRDD = justInterstingWordsRdd.mapToPair(word -> new Tuple2<>(word, 1L));
            JavaPairRDD<String, Long> totals = pairRDD.reduceByKey(Long::sum);
            JavaPairRDD<Long,String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
            JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
            List<Tuple2<Long, String>> results = sorted.take(10);
            results.forEach(System.out::println);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
