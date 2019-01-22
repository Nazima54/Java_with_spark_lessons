package com.jobreadyprogrammer.spark;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.HouseMapper;
import com.jobreadyprogrammer.pojos.House;


public class CsvToDatasetHouseToDataframe {
	
	public void start() {
		
		SparkSession spark = SparkSession.builder()
				.appName("CSV to dataframe to Dataset<House> and back")
				.master("local")
				.getOrCreate();
		String filename = "src/main/resources/houses.csv";
		
		//Injest file into adata frame
			Dataset<Row> df = spark.read().format("csv")
					.option("inferSchema", "true") // Make sure to use string version of true
					.option("header", true)
					.option("sep",";")
					.load(filename);
			System.out.println("This is the house injested in a Dataframe: ");
//			df.show(5);
//			df.printSchema();
			
		// to send data set to pojo
			System.out.println("This is the house injested in a Dataset: ");
			Dataset<House> houseDs = df.map(new HouseMapper(), Encoders.bean(House.class));
//			houseDs.show();
//			houseDs.printSchema();
			
		// Covert house data set into dataframe
			
			Dataset<Row> df2 =  houseDs.toDF();
				df2 = df2.withColumn("formatedDate", concat(df2.col("vacantBy.date"), lit("_"), df2.col("vacantBy.year")));
					df2.printSchema();
					df2.show(10);
	}	    
}

 
   