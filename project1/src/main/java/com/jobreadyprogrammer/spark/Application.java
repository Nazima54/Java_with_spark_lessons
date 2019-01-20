package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application{
	
	public static void main(String args[]){
		
		// Connect to spark master node
		//Create a session
		
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local")
				.getOrCreate();
		
		//Get data
		Dataset<Row> df = spark.read().format("csv")
		.option("header",true)
		.load("src/main/resources/name_and_comments.txt");
		// df.show();
		
		// Transformation rem datasets are immutable 
		// we can generate new dataset by assigning it as df =
		// Create a new column called "full_name" using the "last_name" and "first_name"
		df = df.withColumn("full_name",concat(df.col("last_name"),lit(", "),df.col("first_name")));
		//df.show();

		// Transformation Show all the comments from the dataset that contains numbers in it
		df = df.filter(df.col("comment").rlike("\\d+"));
		//df.show();
		
		// Transformation order by the previous transformation by "last_name" in ascending
		df = df.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		//df.show();
		
		// we can write all the transformations as a single core code
		df = df.withColumn("full_name",
				concat(df.col("last_name"), lit(", "),df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		
		// save the modified dataframe in postgresql database
		String dbConnectionUrl = "jdbc:postgresql://localhost/postgres";
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "postgres");
		prop.setProperty("password", "password");
		
		// write to the dataframe
		df.write()
		.mode(SaveMode.Overwrite)
		.jdbc(dbConnectionUrl, "project1", prop);
		
	}
		
}
	




