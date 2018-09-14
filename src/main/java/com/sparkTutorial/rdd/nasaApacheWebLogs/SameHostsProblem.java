package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
    	SparkConf conf = new SparkConf().setMaster("local").setAppName("UnionLogProblem");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> file1 = sc.textFile("in/nasa_19950701.tsv");
    	JavaRDD<String> file1data = file1.filter(x -> !x.contains("response"));
		//System.out.println(file1data.take(10));
    	JavaRDD<String> file1host = file1data.map(x -> x.split("\t")[0]);
    	
    	JavaRDD<String> file2  = sc.textFile("in/nasa_19950801.tsv");
    	JavaRDD<String> file2data = file2.filter(x -> !x.contains("response"));
    	//System.out.println(file2data.take(10));
    	JavaRDD<String> file2host = file2data.map(x -> x.split("\t")[0]);
    	
    	JavaRDD<String> output = file1host.intersection(file2host);
    	output.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    	
    }
}
