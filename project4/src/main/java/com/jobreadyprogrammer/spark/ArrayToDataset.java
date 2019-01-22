package com.jobreadyprogrammer.spark;

import java.io.Serializable;
//import org.spark_project.dmg.pmml.Row;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ArrayToDataset {

	public void start() {
		
		SparkSession spark = new SparkSession.Builder()
				.appName("Array to Dataset<String>")
				.master("local")
				.getOrCreate();
		
		String[] stringList = new String[] {"Banana","Car", "Glass","Banana", "Computer", "Car"};	
		
		List<String> data = Arrays.asList(stringList);
		
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
		
//		ds.printSchema();
//		ds.show();
//		
//		Dataset<Row> df = ds.groupBy("value").count();
//		df.show(10);
		
//		ds = ds.map(new StringMapper(), Encoders.STRING());
//		ds.show(10);
		
		// We can write map function using lamda function
		
		ds = ds.map((MapFunction<String, String>) row -> " word: " + row, Encoders.STRING());	
		ds.show(10);
		
		// Write a reduce function, reduce takes mumtiple elements and returns as one string.
		
		String stringValue = ds.reduce(new StringReducer());	
		System.out.println(stringValue);
	}
	
	static class StringReducer implements ReduceFunction<String>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String v1, String v2) throws Exception {
			return v1 + v2;
		}
		
}

//	static class StringMapper implements MapFunction<String,String>, Serializable {
//
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public String call(String value) throws Exception {
//			return "word: " + value;
//		}
//		
//	}
	
}



