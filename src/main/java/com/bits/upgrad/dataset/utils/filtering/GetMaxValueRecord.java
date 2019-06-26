package com.bits.upgrad.dataset.utils.filtering;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

/*
// ow to max value and keep all columns 
References:
https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group
https://stackoverflow.com/questions/42636179/how-to-max-value-and-keep-all-columns-for-max-records-per-group
*/	
public class GetMaxValueRecord {
	public static void main(String[] args) {
		SparkSession ss = SparkSession.builder().appName("Reading Schema").master("local[*]").getOrCreate();

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		Dataset<Row> df = ss.read().option("header", false).csv("in\\clusterArtistIDRankin.csv");

		df = df.withColumnRenamed("_c0", "prediction").withColumnRenamed("_c1", "artistID").withColumnRenamed("_c2", "count");
		
		WindowSpec w = Window.partitionBy("prediction").orderBy(df.col("count").desc());
		
		df.withColumn("rank", functions.dense_rank().over(w)).where("rank = 1").drop("rank").show();
		
		/*df.show(); // Displays dataset in tabular format
		df.printSchema(); // prints infered schema from the dataset provided.
*/	}

}
