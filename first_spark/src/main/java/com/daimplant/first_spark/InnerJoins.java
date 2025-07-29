package com.daimplant.first_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class InnerJoins {
    public static void main(String[] args) {
        System.out.println("Testing Joins in Spark");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("Testing Joins Spark Application")
                .setMaster("local[*]");
        try (var sparkContext = new JavaSparkContext(conf)) {
            // Example data for testing joins
            JavaPairRDD<Integer,Integer> visits = sparkContext.parallelizePairs(List.of(
                    new Tuple2<>(4, 18),
                    new Tuple2<>(6, 4),
                    new Tuple2<>(10, 9)
            ));

            JavaPairRDD<Integer,String> users = sparkContext.parallelizePairs(List.of(
                    new Tuple2<>(1, "Aarav"),
                    new Tuple2<>(2, "Ishan"),
                    new Tuple2<>(3, "Yashika"),
                    new Tuple2<>(4, "Raghavi"),
                    new Tuple2<>(5, "Samarth"),
                    new Tuple2<>(6, "Lakshmi"))
            );


            // Perform a join operation
            JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);

            // Print the results of the join
            joinedRdd.foreach(tuple -> System.out.println("Key: " + tuple._1 + ", Value: " + tuple._2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
