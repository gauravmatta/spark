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

		//Reduce Operation Example
		// Create a JavaRDD from the input data
		JavaRDD<Double> myRdd	= sc.parallelize(inputData);
		Double result = myRdd.reduce(Double::sum);
		System.out.println("Reduced Sum of all values: " + result);
		//Reduce Operation Example

		// Fix for multiple physical cpu's getting Not Serializable Exception
		myRdd.collect().forEach(value -> System.out.println("Value: " + value));
		// Fix for multiple physical cpu's getting Not Serializable Exception

		// Map Operation Example
		List<Integer> intData = new ArrayList<>();
		intData.add(35);
		intData.add(12);
		intData.add(90);
		intData.add(20);
		JavaRDD<Integer> intRdd = sc.parallelize(intData);
		JavaRDD<Double> sqrtRdd = intRdd.map(Math::sqrt);
		sqrtRdd.foreach(value -> System.out.println("Mapped Square Root Value: " + value));
		// Map Operation Example

		// Count Operation Example
		long count = sqrtRdd.count();
		System.out.println("Count of Square Root Values: " + count);
		// Count Operation Example

		// Count Using Map and Reduce Example
		long countUsingMapReduce = sqrtRdd.map(value -> 1L).reduce(Long::sum);
		System.out.println("Count Using Map and Reduce: " + countUsingMapReduce);
		// Count Using Map and Reduce Example

		// MapToPair Operation Example
		myRdd.mapToPair(value -> new Tuple2<>(value, value * 2))
				.foreach(tuple -> System.out.println("Value: " + tuple._1 + ", Double: " + tuple._2));
		// MapToPair Operation Example

		sc.close();
	}

}
