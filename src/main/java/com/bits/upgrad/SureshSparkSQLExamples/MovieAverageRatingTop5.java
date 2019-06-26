package com.bits.upgrad.SureshSparkSQLExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class MovieAverageRatingTop5 {
	public static void main(String[] args) {
	
Logger.getLogger("org").setLevel(Level.ERROR);
		
		// For SparkSQL creating a SparkSession object
		
		SparkSession session = SparkSession.builder().appName("Top20Movies").master("local[*]").getOrCreate();
		
		
		// Dataframereader object is required to read data from an external file
        DataFrameReader dataFrameReader = session.read();
        
        //.csv method is used to read data from a csv file.
        
        Dataset<Row> ratings = dataFrameReader.option("header","true").csv("in/u.data");
        System.out.println("=== Print out schema ratings ===");
        ratings.printSchema();
        
        // Select the columns ItemID and Rating from the dataset. Also, cast the type of Rating to double.
        
        Dataset<Row> ratingsLean = ratings.select(ratings.col("ItemID"), ratings.col("Rating").cast("double"));
        System.out.println("=== Print out schema ratingsLean ===");
        ratingsLean.printSchema();
        
        // Groupby on the column ItemID and finding average on the Column Rating
        
        Dataset<Row> ratingsAvg = ratingsLean.groupBy(ratingsLean.col("ItemID")).avg("Rating");
        System.out.println("=== Print out schema ratingsAvg ===");
        ratingsAvg.printSchema();
        
        // Renaming the column avg(Rating) using the "as" method to Avg_Rating
       
        Dataset<Row> ratingsAvgRename = ratingsAvg.select(ratingsAvg.col("ItemID"), ratingsAvg.col("avg(Rating)").as("Avg_Rating"));
        ratingsAvgRename.printSchema();
        
        // Sorting the records based on Avg_Rating in descending order and fetching top 20 records using limit
        
        Dataset<Row> ratingsAvgSorted = ratingsAvgRename.orderBy(ratingsAvgRename.col("Avg_Rating").desc()).limit(20);
        ratingsAvgSorted.show();
        
        // Reading the u.item dataset
        
        Dataset<Row> movieData = dataFrameReader.option("header","true").csv("in/u.item");
        System.out.println("=== Print out schema ratings ===");
        movieData.printSchema();
        
        
        // Fetching the columns ItemID and MovieName from dataset
        
        Dataset<Row> movieDataLean = movieData.select(movieData.col("ItemID"), movieData.col("MovieName"));
        System.out.println("=== Print out schema movieDataLean ===");
        movieDataLean.printSchema();
        
        // Joining ratingsAvgSorted and movieDataLean on ItemID column
        
        Dataset<Row> joinedData = ratingsAvgSorted.join(movieDataLean, ratingsAvgSorted.col("ItemID").equalTo(movieDataLean.col("ItemID")), "inner");
        joinedData.printSchema();
        joinedData.show();
        
        // Fetching the movie name and Avg_Rating
        
        Dataset<Row> finalData = joinedData.select(joinedData.col("MovieName"), joinedData.col("Avg_Rating")).orderBy(joinedData.col("Avg_Rating").desc());
        
        // Displaying 
        finalData.show();
		
		
		
	}

}
