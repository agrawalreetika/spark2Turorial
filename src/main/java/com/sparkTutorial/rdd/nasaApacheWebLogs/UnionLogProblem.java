package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
    	
    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("UnionLogProblem");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> file1 = sc.textFile("in/nasa_19950701.tsv");
    	String header1 = file1.first();
    	JavaRDD<String> file1data = file1.filter(x -> x!=header1);
    	
    	JavaRDD<String> file2  = sc.textFile("in/nasa_19950801.tsv");
    	String header2 = file2.first();
    	JavaRDD<String> file2data = file2.filter(x -> x!=header2);
    	
    	JavaRDD<String> output = file1data.union(file2data);
    	JavaRDD<String> sample = output.sample(true, 0.1);
    	sample.saveAsTextFile("out/sample_nasa_logs.tsv");
    }
}
