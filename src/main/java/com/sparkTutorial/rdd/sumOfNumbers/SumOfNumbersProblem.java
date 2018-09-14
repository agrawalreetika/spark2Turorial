package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
    	Logger.getLogger("org").setLevel(Level.ERROR);
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("SumOfNumbersProblem");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> data = sc.textFile("in/prime_nums.text");
    	
    	JavaRDD<String> numbers = data.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
    	
    	JavaRDD<String> newnumbers = numbers.filter(x -> !x.isEmpty());
    	
    	//JavaRDD<Integer> intnumbers = newnumbers.map(x -> Integer.valueOf(x));
    	JavaRDD<Integer> intnumbers = newnumbers.map(x -> Integer.parseInt(x));
    	
    	Integer sumnumber = intnumbers.reduce((x,y) -> x+y);
    	
    	System.out.println("Sum of numbers:: " + sumnumber);
    	
    }
}
