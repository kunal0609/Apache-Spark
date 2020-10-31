package com.kunal;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//spark config
		SparkConf conf = new SparkConf();
		
		//set mandatory config
		conf.setMaster("local").setAppName("demo");
		
		//create context which is nothing but context of our word count
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//read file from given form
		JavaRDD<String> WordFile = jsc.textFile("/home/developers/eclipse-workspace/Wordcount-Example/index.txt");
		WordFile.foreach(line -> System.out.println(line));
		
		//tokenize the whole file into single single word
		JavaRDD<String> words = WordFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		words.foreach(line -> System.out.println(line));
		
		//convert each word into key which is our case value is 1 in our case
		JavaPairRDD<String, Integer> wordkeyvalue = words.mapToPair(word -> new Tuple2<>(word, 1));
		wordkeyvalue.foreach(line -> System.out.println(line));
		
		//now we will reduce e.i. add all values of same word, which will gives us count of that word
		JavaPairRDD<String, Integer> wordscount = wordkeyvalue.reduceByKey((first, second) -> first+second);
		wordscount.foreach(pair -> System.out.println(pair._1+" "+pair._2));
		
	}

}
