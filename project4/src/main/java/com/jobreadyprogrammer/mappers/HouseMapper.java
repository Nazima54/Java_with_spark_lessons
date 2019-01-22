package com.jobreadyprogrammer.mappers;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.jobreadyprogrammer.pojos.House;


public class HouseMapper implements MapFunction<Row, House>{

	@Override
	public House call(Row value) throws Exception {
		House h = new House();
		h.setId(value.getAs("id"));
		h.setAddress(value.getAs("address"));
		h.setPrice(value.getAs("price"));
		h.setSqft(value.getAs("sqft"));
		
		// Mapping a date field to date
		
		String vacancyDateString = value.getAs("vacantBy").toString();
		
		if(vacancyDateString != null) {
			SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
			h.setVacantBy(parser.parse(vacancyDateString));
		}
		return h;
	}
	
}

