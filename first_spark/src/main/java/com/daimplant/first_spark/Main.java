package com.daimplant.first_spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple22;
import scala.Tuple5;

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

		// Tuple2 Example
		Tuple2<Integer, Double> myTuple = new Tuple2<>(9,3.0);
		System.out.println("Tuple Value: " + myTuple._1 + ", Tuple Value: " + myTuple._2);
		// Tuple2 Example

		// Map Operation Example with Tuple2
		JavaRDD<Tuple2<Integer,Double>> sqrtTupleRdd = intRdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));
		sqrtTupleRdd.foreach(tuple -> System.out.println("Integer Value: " + tuple._1 + ", Square Root Value: " + tuple._2));
		// Map Operation Example with Tuple2

		// Tuple22 Example Biggest Tuple in Scala
		Tuple22<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> tuple22 =	new Tuple22<>(1,2,3,4,5,6,7,8,9,10,
				11,12,13,14,15,16,17,18,19,20,
				21,22);
		System.out.println("Tuple22 Values: " + tuple22);
		// Tuple22 Example Biggest Tuple in Scala

		// MapToPair Operation Example with Tuple2
		myRdd.mapToPair(value -> new Tuple2<>(value, value * 2))
				.foreach(tuple -> System.out.println("Value: " + tuple._1 + ", Double: " + tuple._2));
		intRdd.mapToPair(value -> new Tuple2<>(value,Math.sqrt(value))).foreach(tuple -> System.out.println("Integer Value: " + tuple._1 + ", Double: " + tuple._2));
		// MapToPair Operation Example with Tuple2

		sc.close();
	}

}
