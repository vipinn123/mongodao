package com.spring.data.fetcher.mongodao;

import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Accumulators.min;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.addFields;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Sorts.ascending;

import java.io.IOException;


import org.bson.Document;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;

@Configuration
@Controller // This means that this class is a Controller
@RequestMapping(path="/trade") // This means URL's start with /demo (after Application path)
public class MongoDaoController{
	
	private static Logger logger = LoggerFactory.getLogger(MongoDaoController.class);
	
	@Autowired
	private static MongoDatabase mongoDatabase;
	
	
	
	@Bean
	public MongoDatabase getMongoDatabase(@Value("${spring.data.mongodb.uri}") String connectionString) {
	      
		MongoClient mongoClient = new MongoClient(
				    new MongoClientURI(connectionString)
				);
				
		mongoDatabase = mongoClient.getDatabase("trade_db");	
	    return mongoDatabase;
    }

	
	
	@GetMapping(path="/aggregatedPullFromTickBatch")
	public @ResponseBody List<Document> aggregatedPullFromTickBatch ( @RequestParam String startDate
	      , @RequestParam String endDate
	      , @RequestParam int interval ) throws JSONException, IOException, ParseException {
		
		logger.info("Entering /aggregatedPullFromTickBatch");
		
		List <Document> result1;
			
		logger.info("startDate : "+ startDate);
		logger.info("endDate : "+ endDate);
		logger.info("interval : "+ interval);
	
		result1 = mongodbFetchFromTickData(startDate,endDate,interval);

		logger.info("Exiting /aggregatedPullFromTickBatch");
		
		return result1;
	}
	
	@GetMapping(path="/aggregatedPullFromMinuteBatch")
	public @ResponseBody List<Document> aggregatedPullFromMinuteBatch ( @RequestParam String startDate
	      , @RequestParam String endDate
	      , @RequestParam int interval ) throws JSONException, IOException, ParseException {
		
		logger.info("Entering /aggregatedPullFromMinuteBatch");
		
		List <Document> result1;
			
		logger.info("startDate : "+ startDate);
		logger.info("endDate : "+ endDate);
		logger.info("interval : "+ interval);
	
		result1 = mongodbFetchFromMinuteData(startDate,endDate,interval);

		logger.info("Exiting /aggregatedPullFromMinuteBatch");
		
		return result1;
	}
	
	@GetMapping(path="/aggregatedMinuteLoad")
	public @ResponseBody Document aggregatedMinuteLoad ( @RequestParam String startDate
	      , @RequestParam String endDate
	      , @RequestParam int interval ) throws JSONException, IOException, ParseException {
		
		logger.info("Entering /aggregatedMinuteLoad");
		
		List <Document> result1;
			
		logger.info("startDate : "+ startDate);
		logger.info("endDate : "+ endDate);
		logger.info("interval : "+ interval);
	
		result1 = mongodbFetchFromTickData(startDate,endDate,1);
		
		if(result1.size() > 0) {
	        MongoCollection<Document> minute_collection = mongoDatabase.getCollection("minute_data");
	        minute_collection.insertMany(result1);
	    }

		logger.info("Exiting /aggregatedMinuteLoad");
		
		return new Document("count",result1.size());
	  }
	
	public static List<Document>  mongodbFetchFromTickData(String fromDate, String toDate, int interval) throws ParseException {
		
			MongoCollection<Document> collection = mongoDatabase.getCollection("tick_data");
			
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd");
	        /*fromDate ="2020-07-03"; 
	        toDate ="2020-07-04";*/
	        Date startDate = simpleDateFormat.parse(fromDate);
	        Date endDate = simpleDateFormat.parse(toDate);
	        
	        
	        AggregateIterable<Document> result1 = collection.aggregate(Arrays.asList(
	        		match(and(gte("tickTimestamp",startDate),lte("tickTimestamp",endDate))),
	        		eq("$addFields", eq("tickDate", eq("$dateToParts", new Document().append("date", "$tickTimestamp").append("timezone", "+0530")))),
	        		eq("$addFields",eq("tickDate.interval",new Document("$subtract",Arrays.asList("$tickDate.minute",new Document("$mod",Arrays.asList("$tickDate.minute",interval)))))),
	        		project(exclude("tickDate.millisecond", "tickDate.second","tickDate.minute")),
	        		//project(exclude("tickDate.millisecond", "tickDate.second")),
	        		sort(ascending("tickTimestamp")),
					group(new Document().append("tickDate", "$tickDate").append("token", "$token"),
							max("highlastTradedPrice", "$lastTradedPrice"), min("lowlastTradedPrice", "$lastTradedPrice"), 
							first("openlastTradedPrice", "$lastTradedPrice"), last("closelastTradedPrice", "$lastTradedPrice"), 
							sum("totalBuyQuantity_Minute", "$totalBuyQuantity"), sum("totalSellQuantity_Minute", "$totalSellQuantity"), 
							sum("volumeTradedToday_Minute", "$volumeTradedToday"), sum("depth_buy_price_avg_Minute", "$depth_buy_price_avg"), 
							sum("depth_buy_orders_avg_Minute", "$depth_buy_orders_avg"), sum("depth_buy_quantity_avg_Minute", "$depth_buy_quantity_avg"), 
							sum("depth_sell_price_avg_Minute", "$depth_sell_price_avg"), sum("depth_sell_orders_avg_Minute", "$depth_sell_orders_avg"), 
							sum("depth_sell_quantity_avg_Minute", "$depth_sell_quantity_avg")),
					sort(ascending("_id.tickDate.hour","_id.tickDate.interval"))
	       		)).allowDiskUse(true);
	    
	

	    Iterator<Document> iter = result1.iterator();
	    List<Document> resultInserts = new ArrayList<Document>();
        iter.forEachRemaining(resultInserts::add); 

		return resultInserts;
			
	}
	
	public static List<Document>  mongodbFetchFromMinuteData(String fromDate, String toDate, int interval2) throws ParseException {
		
		MongoCollection<Document> collection = mongoDatabase.getCollection("minute_data");
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd");
        /*fromDate ="2020-07-03"; 
        toDate ="2020-07-04";*/
        Date startDate = simpleDateFormat.parse(fromDate);
        Date endDate = simpleDateFormat.parse(toDate);
        

        AggregateIterable<Document> result1 = collection.aggregate(Arrays.asList(
        addFields(new Field("time", 
        	    new Document("$dateFromParts", 
        	    new Document("year", "$_id.tickDate.year")
        	                .append("month", "$_id.tickDate.month")
        	                .append("day", "$_id.tickDate.day")
        	                .append("hour", "$_id.tickDate.hour")
        	                .append("minute", "$_id.tickDate.interval")))), 
        match(and(Arrays.asList(gte("time", 
        	                startDate), lte("time",endDate)))),
        eq("$addFields",eq("_id.tickDate.minute",new Document("$subtract",Arrays.asList("$_id.tickDate.interval",new Document("$mod",Arrays.asList("$_id.tickDate.interval",interval2)))))),
		project(exclude("_id.tickDate.millisecond", "_id.tickDate.second","_id.tickDate.interval")))

        );
        
	    Iterator<Document> iter = result1.iterator();
	    List<Document> resultInserts = new ArrayList<Document>();
	    iter.forEachRemaining(resultInserts::add); 
	
		return resultInserts;
		
	}
	
	
	@Scheduled(cron="0 45 15 * * MON-FRI",zone="IST")
	public void dailyBatchAggregateLoad() throws ParseException  {
		
		logger.info("Entering dailyBatchAggregateLoad() cron");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("The time is now :" + dateFormat.format(new Date()));
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd");		
	    Date date = new Date();  
	    System.out.println(date);
	    String fromDate = simpleDateFormat.format(date);
	    
	    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("IST"));
	    c.setTime(date); 
	    c.add(Calendar.DATE, 1);
	    System.out.println(c.getTime());
	    String toDate = simpleDateFormat.format(c.getTime());
	    
	    
	    logger.info("Starting Minute Aggregation for Date:" + fromDate + " to " + toDate);
	    
	    int interval = 1;
	    List<Document> resultInserts= mongodbFetchFromTickData(fromDate, toDate, interval);
        
	    if(resultInserts.size() > 0) {
	        MongoCollection<Document> minute_collection = mongoDatabase.getCollection("minute_data");
	        minute_collection.insertMany(resultInserts);
	    }
		
		logger.info("Exiting dailyBatchAggregateLoad() cron");
        
	}

	
	
	
	


	
}
