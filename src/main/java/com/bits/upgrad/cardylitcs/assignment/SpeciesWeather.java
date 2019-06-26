package com.bits.upgrad.cardylitcs.assignment;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;



/*****************************************************************************************************
 * There are two text files
 *
 * plants.txt
 * weather.txt
 *
 * plants.txt is a pipe delimited list of different types of plant species and their types.
 * weather.txt is a pipe delimited list of different plant types and the climate that they thrive.
 *
 * Display a table with two columns, Species and Weather, displaying only the species that thrives in one type of climate.
 *
 * Example:
 *
 * plants.txt
 * species a|type a
 * species b|type b
 *
 * weather.txt
 * type a|climate a
 * type b|climate a
 * type b|climate b
 *
 * final answer:
 *
 * Species|Weather
 * species a|climate a
 *
 * *** Notes
 * * You may use spark sql or dataset/frame APIs.
 * * You may not use the spark context or the sql context directly.
 * * Bonus points if you provide 2 sets of answers using API and Spark SQL.
 * * Try to avoid using RDD APIs if possible.
 * * If you end up using APIs at any point, please print the type of your variable after each set of transformations to show whether they're RDDs, Dataframes, or Datasets.
 * * Don't worry about compiling and deploying the project anywhere.  We will execute the code in the IDE.
 ****************************************************************************************************/

public class SpeciesWeather {
    public static void main(String[] args) {
        // System.out.println("Hello Spark");

        SparkSession spark = SparkSession
                .builder()
                .appName("Spices and Weather")
                .config("spark.master", "local[*]")
                .getOrCreate();


        Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getRootLogger().setLevel(Level.ERROR);

        // file location should be passed as an argument from command prompt ideally
        Dataset<Row> plantsdata = spark.read().format("text").option("delimiter", "|").csv("in\\plants.txt");

        System.out.println("Plant dataframe content follows after reading from source:");
        // plantsdata.show();

        // renaming the default column names
        Dataset<Row> namedplantsdata = plantsdata.withColumnRenamed("_c0", "species").withColumnRenamed("_c1", "type");

        System.out.println("Plant dataframe columns renamed to apt column names:");
        // namedplantsdata.show();

        // file location should be passed as an argument from command prompt ideally
        Dataset<Row> weatherdata = spark.read().format("text").option("delimiter", "|").csv("in\\weather.txt");

        System.out.println("weather dataframe content follows after reading from source:");
        // weatherdata.show();

        // renaming the default column names
        Dataset<Row> namedweatherdata = weatherdata.withColumnRenamed("_c0", "type").withColumnRenamed("_c1", "climate");

        System.out.println("weather dataframe columns renamed to apt column names:");
        // namedweatherdata.show();

        // joining two datasets with a join condition - we can use broadcast if one of the dataset is small
        Dataset<Row> joinedPlantsSepcies = namedplantsdata.join(namedweatherdata, namedplantsdata.col("type").equalTo(namedweatherdata.col("type"))).drop(namedweatherdata.col("type")).drop(namedplantsdata.col("type"));

        System.out.println("joining plant dataframe and weather dataframe on type column, discarding repeated columns:");
        joinedPlantsSepcies.sort("species").show();
        
        WindowSpec winSpec = Window.partitionBy(joinedPlantsSepcies.col("species")).orderBy(joinedPlantsSepcies.col("climate"));
        
        // finding out species which have only one climate
        Dataset<Row> joinedPlantsSepciesGrouped = joinedPlantsSepcies.withColumn("rank", functions.dense_rank().over(winSpec));
        
        joinedPlantsSepciesGrouped.groupBy(joinedPlantsSepciesGrouped.col("species")).sum("rank").as("sum").show();;

        System.out.println("Finding out species with only one climate:");
        //joinedPlantsSepciesGrouped.show();


        //Dataset<Row> resultDataset = joinedPlantsSepcies.join(joinedPlantsSepciesGrouped, joinedPlantsSepcies.col("species").equalTo(joinedPlantsSepciesGrouped.col("species"))).drop(joinedPlantsSepcies.col("species")).drop(joinedPlantsSepciesGrouped.col("count"));

        System.out.println("Joined joinedPlantsSepcies and joinedPlantsSepciesGrouped to get desired output");

        // ordering columns in the way we wanted
        // resultDataset.select("species","climate").show();

        /*
        calcuated answer for the dataset given :
        +------------+-------+
        |     species|climate|
        +------------+-------+
        |       roses|  sunny|
        |     violets|  sunny|
        |    tomatoes|   cool|
        |strawberries|   cool|
        | blueberries|   cool|
        |     pansies|  sunny|
        |      cactus|  sunny|
        +------------+-------+

         */

        // writing resultant output to an output location, this can be passed from command prompt as an argument
        //resultDataset.write().save("D:\\test\\output");

    }
}
