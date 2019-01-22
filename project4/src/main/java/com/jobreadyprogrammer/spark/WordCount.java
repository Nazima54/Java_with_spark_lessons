// Our goal here is to Parse the text and find the most commonly used text by Shakespeare
// This code can be used for any big cluster not just on this tyep
// eg use the same code to find out the most common word on the whole Internet

		
package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.jobreadyprogrammer.mappers.LineMapper;

public class WordCount {

	public void start() {
		
		 String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" + 
			  		"'for', 'if', 'in', 'into', 'is', 'it',\r\n" + 
			  		"'no', 'not', 'of', 'on', 'or', 'such',\r\n" + 
			  		"'that', 'the', 'their', 'then', 'there', 'these',\r\n" + 
			  		"'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," + 
			  		"'your', 'you', 'I', "
			  		+ " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
		 
		 
		SparkSession  spark = SparkSession.builder()
				.appName("unstructured text to flatmap")
				.master("local")
				.getOrCreate();
		
		String filename = "src/main/resources/shakespeare.txt";
		
		Dataset<Row> df = spark.read().format("text")
				.load(filename);
		
		// Parse the text and find the most commonly used text by Shakespeare
		
		// First we will print the schema nd show the first 10 lines to see what the data looks like
		//		df.printSchema();
		//		df.show(10);
		
		// Flatmap() takes in one input and produces multiple outputs
		
		 Dataset<String> wordDS = df.flatMap(new LineMapper(), Encoders.STRING());
		 // wordDS.show(100);
		 
		// Convert the wordDS dataset into dataframe and use groupBy function to add acount column
		 Dataset<Row> df2 = wordDS.toDF();
		 df2 = df2.groupBy("value").count();
		 
		 // Use orderBy to arrange the the count in asc/desc order
		 df2 = df2.orderBy(df2.col("count").desc());
		 
		 // Get rid of boring words
		 df2 = df2.filter("lower(value) Not IN " + boringWords);
		 
		 
		 df2.show(500);

	}
	

}
