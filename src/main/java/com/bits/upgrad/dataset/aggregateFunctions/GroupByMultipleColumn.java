package com.bits.upgrad.dataset.aggregateFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import com.bits.upgrad.dataset.utils.mine.UtilsMine;

// My Tip: Understand groupby interms of tree structure, grouping each column items at one tree level.
public class GroupByMultipleColumn {

	public static void main(String[] args) {
		
		UtilsMine.ignoreLogsFromPackagesExceptError();
		SparkSession sparkSession = SparkSession.builder() // With Spark 2.0 new entry point is SparkSession,
															// SparkContext,SQLContext,HiveContext,SparkCong are wrapped
															// inside SparkSession
				.master("local").appName("Grop by Multiple Columns").getOrCreate();
		
		Dataset<Row> employeeDetails = sparkSession.read()
				.option("head", false)
				.option("inferschema", true)
				.csv("in/employeeDetails.csv")
				.withColumnRenamed("_c0", "Name")
				.withColumnRenamed("_c1", "age")
				.withColumnRenamed("_c2", "role")
				.withColumnRenamed("_c3", "salary")
				.withColumnRenamed("_c4", "zipcode");
		
		employeeDetails.show();
		
		StructType schema = employeeDetails.schema();
		
		for(String fieldName : schema.fieldNames()) {
			System.out.println("columnName"+ fieldName);
		}
		
		// >>>>>>> Grop Employees by Role
		System.out.println("Group Employees by Role");		
		employeeDetails.groupBy("role").count().show();
		// Another way
		employeeDetails.select("role").groupBy("role")
				.agg(functions.count("*").alias("Frequency")).show();

		// >>>>>>>>>> Grop Employees by Role and Zipcode
		System.out.println("Group Employees by Role and Zipcode");
		employeeDetails.groupBy("role","zipcode").count().show();

		// >>>>>>>>>> Grop Employees by Role, Zipcode and Age
		System.out.println("Group Employees by Role, Zipcode and Age");
		employeeDetails.groupBy("role","zipcode","age").count().show();
	}

}
