package com.upgrad.sparkassignment2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkFilter {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("FilterBy");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = sc.textFile(args[0]);

		JavaRDD<String> outRDD = fileRDD.filter(x -> {
			String[] str = x.split(",");
			if (str.length > 6 && str[5].equals("4"))
				return true;
			else
				return false;
		});

		outRDD.saveAsTextFile(args[1]);

		sc.close();

	}

}
