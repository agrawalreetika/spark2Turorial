package com.sparkTutorial.rdd.airports;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.rdd.commons.Utils;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
    	SparkConf config = new SparkConf().setAppName("AirportUsecase").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(config);
    	
    	JavaRDD<String> data = sc.textFile("in/airports.text");
    	
    	JavaRDD<String> usdata = data.filter(x->x.contains("United States"));
    	
    	JavaRDD<String> output = usdata.map(x -> {
    		 						 String [] arr = x.split(Utils.COMMA_DELIMITER);
    		 						 return StringUtils.join(new String[]{arr[1], arr[2]}, ",");
    	 				   });
    	output.saveAsTextFile("out/airports_in_usa.text");
    }
}
