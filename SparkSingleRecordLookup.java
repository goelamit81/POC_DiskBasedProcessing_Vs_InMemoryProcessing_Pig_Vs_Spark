package com.upgrad.sparkassignment1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSingleRecordLookup {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SingleRecordLookup");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = sc.textFile(args[0]);

		JavaRDD<String> outRDD = fileRDD.filter(x -> {
			String[] str = x.split(",");
			if (str.length > 5 && str[0].equals("2") && str[1].equals("2017-10-01 00:15:30")
					&& str[2].equals("2017-10-01 00:25:11") && str[3].equals("1") && str[4].equals("2.17"))
				return true;
			else
				return false;
		});

		outRDD.saveAsTextFile(args[1]);

		sc.close();

	}

}
