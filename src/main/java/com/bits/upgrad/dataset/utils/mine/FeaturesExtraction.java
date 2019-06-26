package com.bits.upgrad.dataset.utils.mine;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class FeaturesExtraction {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

		
		// Create a SparkSession
		SparkSession sc = SparkSession.builder().appName("KMeansCluster").master("local").getOrCreate();

		// Loads data
		Dataset<Row> csa = sc.read().option("header", false).format("csv")
				// .load("C:\\Topic\\bits\\Cours6-IntrotoBigdataAnalytics\\Mod5-SaavnAnalyticsProject\\input\\activity\\sample100mb.csv");
				.load("C:\\Topic\\bits\\Cours6-IntrotoBigdataAnalytics\\Mod5-SaavnAnalyticsProject\\input\\activity\\csa-subset3.csv");
		// total no of distinct user records in csa-subset3.csv: 46456
		
		// Give names to colums for click stream data
		Dataset<Row> namedcsa = csa.withColumnRenamed("_c0", "userID").withColumnRenamed("_c1", "timestamp")
				.withColumnRenamed("_c2", "songID").withColumnRenamed("_c3", "date").select("userID", "songID", "date");

		/*
		 * //Find number of distinct user IDs
		 * System.out.println("No of distinct user IDs:" + namedcsa.select("userID").distinct().count());
		 */
		
		/*
		 * write sorted dataset to a file
		Dataset<Row> sortedDataFrame = namedcsa.sort("userID", "songID", "date");
		sortedDataFrame.show();
		sortedDataFrame.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").save("C:\\Topic\\bits\\Cours6-IntrotoBigdataAnalytics\\Mod5-SaavnAnalyticsProject\\outputSortedUserIdSongIDDate"); 
		 */
		
		// Sorting in descending order of user count
//		namedcsa.groupBy("userID").agg(functions.count("*").alias("userCount")).sort(functions.desc("userCount")).show();
		
		//1. Total Number of distinct songs listened by a user
		/*Dataset<Row> distintSongsListened = namedcsa.groupBy("userID", "songID").count().groupBy("userID").agg(functions.count("*").alias("distintSongsListened"));
		distintSongsListened.show();
		System.out.println("distintSongsListened - no of records:" + distintSongsListened.count());*/
		
		//2. Average Number of songs listened by a user
		/*Dataset<Row> avgSongsListened = namedcsa.groupBy("userID","songID").count().groupBy("userID").agg(functions.avg("count").alias("avgSongsListened"));
		avgSongsListened.show();
		System.out.println("avgSongsListened - no of records:" + avgSongsListened.count());*/
		
		
		// 4. Number of Times each song is listened by a user
		Dataset<Row> noOfTimesEachSongListened = namedcsa.groupBy("userID", "songID").count().withColumnRenamed("count","noOfTimesEachSongListened");
		noOfTimesEachSongListened.show();
		System.out.println("noOfTimesEachSongListened - no of records:" +noOfTimesEachSongListened.count());
		
		// 5. When was the last song listened by the user
		// below thing can be achieved even with function.to_date instead of functions.unix_timestamp
		/*Dataset<Row> lastdate = namedcsa.withColumn("lastdate", functions.datediff(functions.current_timestamp(), functions.unix_timestamp(namedcsa.col("date"), "yyyymmdd").cast(DataTypes.TimestampType)));
		Dataset<Row> lastDateDateSet = lastdate.groupBy("userID").agg(functions.min("lastdate").alias("recency"));
		lastDateDateSet.show();
		System.out.println("recency - no of records:" + lastDateDateSet.count());*/
		
		// 6. Number of Transactions made by each user
		/*Dataset<Row> noOFUserTrans = namedcsa.groupBy("userID").count().withColumnRenamed("count","distintSongsListened");
		noOFUserTrans.show();
		
		System.out.println("noOFUserTrans - no of records:" + noOFUserTrans.count());*/
		
		// 7. Number of Songs listened by an user on an average
		
		
		

		

	}

}
