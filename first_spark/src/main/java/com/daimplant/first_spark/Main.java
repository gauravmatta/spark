package com.daimplant.first_spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(98.32);
		inputData.add(20.32);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("First Spark Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Double> myRdd	= sc.parallelize(inputData);		
		myRdd.mapToPair(value -> new Tuple2<>(value, value * 2))
				.foreach(tuple -> System.out.println("Value: " + tuple._1 + ", Double: " + tuple._2));
		sc.close();
	}

}
