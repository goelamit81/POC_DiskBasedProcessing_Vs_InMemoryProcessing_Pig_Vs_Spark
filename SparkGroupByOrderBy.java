package com.upgrad.sparkassignment3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class SparkGroupByOrderBy {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("GroupByOrderBy");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> fileRDD = sc.textFile(args[0]);

		JavaRDD<String> outRDD = fileRDD.filter(x -> {
			String[] str = x.split(",");
			if (str.length > 9 && !str[9].equals("payment_type") && !str[9].equals(""))
				return true;
			else
				return false;
		});

		JavaPairRDD<String, Integer> out1RDD = outRDD.mapToPair(x -> {
			String[] str = x.split(",");
			return new Tuple2<String, Integer>(str[9], 1);
		});

		JavaPairRDD<String, Integer> out2RDD = out1RDD.reduceByKey((a, b) -> a + b);
		JavaPairRDD<Integer, String> out3RDD = out2RDD.mapToPair(x -> x.swap()).sortByKey();
		JavaPairRDD<String, Integer> finalRDD = out3RDD.mapToPair(x -> x.swap());

		finalRDD.coalesce(1).saveAsTextFile(args[1]);
		sc.close();

	}

}
