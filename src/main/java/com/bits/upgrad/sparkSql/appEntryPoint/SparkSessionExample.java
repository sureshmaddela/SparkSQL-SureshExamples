package com.bits.upgrad.sparkSql.appEntryPoint;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators.AsJava;
import scala.collection.immutable.Map;

public class SparkSessionExample {
	public static void main(String[] args) {
		// SparkSession uses 
		SparkSession sparkSession = SparkSession.builder() // With Spark 2.0 new entry point is SparkSession, SparkContext,SQLContext,HiveContext,SparkCong are wrapped inside SparkSession
		.master("local")
		.appName("Spark Session Example")
		.enableHiveSupport() // With Spark 2.0 Hive support is not given to SparkSession by Default - you need to enable it
		.config("spark.driver.memory", "2G") // configuration paramters which used to be configured before 2.0 using SparkConf are done with config()
		.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse")
		.getOrCreate();
		
		sparkSession.conf().set("spark.driver.memory", "3G");
		
		SparkContext sparkContext = sparkSession.sparkContext(); // you can get the control of SparkContext and SparkConf this way.
		SparkConf conf = sparkSession.sparkContext().getConf();
		
		Map<String, String> all = sparkSession.conf().getAll();
		 System.out.println(JavaConverters.mapAsJavaMapConverter(all).asJava().get("spark.driver.memory"));
		 
		
		
		
		
		
	}
}
