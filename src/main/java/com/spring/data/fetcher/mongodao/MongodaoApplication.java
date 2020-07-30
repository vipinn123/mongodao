package com.spring.data.fetcher.mongodao;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.text.ParseException;
import java.text.SimpleDateFormat;

//import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.mod;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.count;





import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.last;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Accumulators.min;
import static com.mongodb.client.model.Accumulators.sum;


import static com.mongodb.client.model.Projections.exclude;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
//import com.spring.kafka.consumers.DataLoaderConsumerSpringApplication;
import com.spring.trade.entities.ZerodhaTick;
import com.spring.trade.entities.ZerodhaTickMongoRepository;


import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.Document;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.BooleanOperators;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
@EnableScheduling
public class MongodaoApplication implements ApplicationRunner{
	
	private static Logger logger = LoggerFactory.getLogger(MongodaoApplication.class);


	public static void main(String[] args) throws ParseException {
		
		SpringApplication.run(MongodaoApplication.class, args);
		
	}
	
	@Override
	public void run(ApplicationArguments arg0) throws Exception {
	      
		System.out.println("Starting Minute Batch Load");
		//mongodbFetch();
	}
	
	


	/*public static AggregateIterable<Document>  mongodbFetch() throws ParseException {
		MongoClient mongoClient = new MongoClient(
			    new MongoClientURI(
			        "mongodb://localhost:27017/trade_db"
			    )
			);
			MongoDatabase database = mongoClient.getDatabase("trade_db");
			MongoCollection<Document> tick_collection = database.getCollection("tick_data");
			
			
			
			
	
			
			
		    
		    
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd");
            String fromDate ="2020-07-03"; 
            String toDate ="2020-07-04"; 
            Date startDate = simpleDateFormat.parse(fromDate);
            Date endDate = simpleDateFormat.parse(toDate);
            
            int interval = 15;
            
            AggregateIterable<Document> result1 = tick_collection.aggregate(Arrays.asList(
            		match(and(gte("tickTimestamp",startDate),lte("tickTimestamp",endDate))),
            		eq("$set", eq("tickDate", eq("$dateToParts", new Document().append("date", "$tickTimestamp").append("timezone", "+0530")))),
            		eq("$set",eq("tickDate.interval",new Document("$subtract",Arrays.asList("$tickDate.minute",new Document("$mod",Arrays.asList("$tickDate.minute",interval)))))),
            		//project(exclude("tickDate.millisecond", "tickDate.second","tickDate.minute")),
            		project(exclude("tickDate.millisecond", "tickDate.second")),
            		sort(ascending("tickTimestamp")),
    				group(new Document().append("tickDate", "$tickDate").append("token", "$token"),
    						max("highlastTradedPrice", "$lastTradedPrice"), min("lowlastTradedPrice", "$lastTradedPrice"), 
    						first("openlastTradedPrice", "$lastTradedPrice"), last("closelastTradedPrice", "$lastTradedPrice"), 
    						sum("totalBuyQuantity_Minute", "$totalBuyQuantity"), sum("totalSellQuantity_Minute", "$totalSellQuantity"), 
    						sum("volumeTradedToday_Minute", "$volumeTradedToday"), sum("depth_buy_price_avg_Minute", "$depth_buy_price_avg"), 
    						sum("depth_buy_orders_avg_Minute", "$depth_buy_orders_avg"), sum("depth_buy_quantity_avg_Minute", "$depth_buy_quantity_avg"), 
    						sum("depth_sell_price_avg_Minute", "$depth_sell_price_avg"), sum("depth_sell_orders_avg_Minute", "$depth_sell_orders_avg"), 
    						sum("depth_sell_quantity_avg_Minute", "$depth_sell_quantity_avg")),
    				//sort(ascending("_id.tickDate.year,_id.tickDate.month,_id.tickDate.day,_id.tickDate.hour"))
    				sort(ascending("_id.tickDate.hour","_id.tickDate.interval"))
    				//count()
           		)).allowDiskUse(true);

        Iterator<Document> iter = result1.iterator();
        
        List<Document> resultInserts = new ArrayList<Document>();
        iter.forEachRemaining(resultInserts::add); 
        
        /*MongoCollection<Document> minute_collection = database.getCollection("minute_data");
        minute_collection.insertMany(resultInserts);
			
		result1 = null;
		return result1;
			
	}*/
	
	
	
	
}

