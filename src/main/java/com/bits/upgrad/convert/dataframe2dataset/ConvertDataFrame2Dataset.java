package com.bits.upgrad.convert.dataframe2dataset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ConvertDataFrame2Dataset {

	/*
	 * 
	 * Java Encoders are specified by calling static methods on Encoders.
	 * 
	 * List<String> data = Arrays.asList("abc", "abc", "xyz"); 
	 * Dataset<String> ds = context.createDataset(data, Encoders.STRING()); 
	 * 
	 * Encoders can be composed into tuples: 
	 * Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(),
	 * Encoders.STRING()); 
	 * 
	 * List<Tuple2<Integer, String>> data2 = Arrays.asList(new scala.Tuple2(1, "a");
	 * 
	 * Dataset<Tuple2<Integer, String>> ds2 = context.createDataset(data2, encoder2);
	 * 
	 * Or constructed from Java Beans by Encoders#bean:
	 * Encoders.bean(MyClass.class);
	 *
	 */

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkSession ss = SparkSession.builder().master("local[*]").appName("ConvertDataFrame2DataSet").getOrCreate();

		Dataset<Row> input = ss.read().format("csv").option("delimiter", "|").load("in\\plants.txt");

		Dataset<Row> inputnamed = input.withColumnRenamed("_c0", "plant").withColumnRenamed("_c1", "species");
		
		List<Integer> inputList = Arrays.asList(1,2,3,32,3,3,34,23,23,2312,34);
		Dataset<Integer> datasetFromCollection = ss.createDataset(inputList, Encoders.INT());
		datasetFromCollection.printSchema();
		/*
		Map<Integer,String> inputMap = new HashMap<Integer,String>();
		inputMap.put(10, "suresh");
		inputMap.put(20, "kumar");
		
		Dataset<Integer,String> datasetFromCollection = ss.createDataset(inputMap, Encoders.tuple(Encoders.INT(), Encoders.STRING()));
		datasetFromCollection.printSchema();
		*/
		
		// Encoders tuples
		Encoder<Tuple2<String, String>> plantsEncoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
		Dataset<Tuple2<String, String>> plantsDataset = inputnamed.as(plantsEncoder);
		
		// Dataset created from Javabean using Encoders.bean
		Encoder<UserTweetBean>plantEncoder = Encoders.bean(UserTweetBean.class);		
		Dataset<UserTweetBean> plantDataset = inputnamed.as(plantEncoder);

		plantDataset.printSchema();

	}

}
