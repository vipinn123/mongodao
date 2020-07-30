package com.spring.trade.entities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;






import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "tick_data")
public class ZerodhaTick {
	
	//@Id
	//@GeneratedValue(strategy=GenerationType.AUTO)
	//private Long id;
	
	private double closePrice;
	private double openPrice;
	private double averageTradePrice;
	private double change;
	private double highPrice;
	private double lowPrice;
	private long token;
	private double lastTradedPrice;
	private double lastTradeQuantity;
	private Date lastTradedTime;
	
	private double oi;
	private double openInterestDayHigh;
	private double openInterestDayLow;
	private Date tickTimestamp;
	private double totalBuyQuantity;
	private double totalSellQuantity;
	private double volumeTradedToday;
	private boolean tradable;

	private double depth_buy_orders_avg;
	private double depth_buy_quantity_avg;
	private double depth_buy_price_avg;
	
	private double depth_sell_orders_avg;
	private double depth_sell_quantity_avg;
	private double depth_sell_price_avg;

	private double depth_buy_price_range_percent;

	private double depth_sell_price_range_percent;

	
	//Map<String,ArrayList<ZerodhaTickDepth>> depthMap = new LinkedHashMap<String,ArrayList<ZerodhaTickDepth>>();
	/*@OneToMany(mappedBy = "zerodha_tick_data", fetch = FetchType.LAZY,
	          cascade = CascadeType.ALL)*/
	private Map<String,ArrayList<ZerodhaTickDepth>> depth;

	public ZerodhaTick() {
		// TODO Auto-generated constructor stub
	}

	public double getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(double closePrice) {
		this.closePrice = closePrice;
	}

	public double getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(double openPrice) {
		this.openPrice = openPrice;
	}

	public double getAverageTradePrice() {
		return averageTradePrice;
	}

	public void setAverageTradePrice(double averageTradePrice) {
		this.averageTradePrice = averageTradePrice;
	}

	public double getChange() {
		return change;
	}

	public void setChange(double change) {
		this.change = change;
	}

	public double getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(double highPrice) {
		this.highPrice = highPrice;
	}

	public double getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(double lowPrice) {
		this.lowPrice = lowPrice;
	}

	public long getInstrumentToken() {
		return token;
	}

	public void setInstrumentToken(long instrumentToken) {
		this.token = instrumentToken;
	}

	public double getLastTradedPrice() {
		return lastTradedPrice;
	}

	public void setLastTradedPrice(double lastTradedPrice) {
		this.lastTradedPrice = lastTradedPrice;
	}

	public double getLastTradedQuantity() {
		return lastTradeQuantity;
	}

	public void setLastTradedQuantity(double lastTradedQuantity) {
		this.lastTradeQuantity = lastTradedQuantity;
	}

	public Date getLastTradedTime() {
		return lastTradedTime;
	}

	public void setLastTradedTime(Date lastTradedTime) {
		this.lastTradedTime = lastTradedTime;
	}

	public double getOi() {
		return oi;
	}

	public void setOi(double oi) {
		this.oi = oi;
	}

	public double getOpenInterestDayLow() {
		return openInterestDayLow;
	}

	public void setOpenInterestDayLow(double openInterestDayLow) {
		this.openInterestDayLow = openInterestDayLow;
	}

	public double getOpenInterestDayHigh() {
		return openInterestDayHigh;
	}

	public void setOpenInterestDayHigh(double openInterestDayHigh) {
		this.openInterestDayHigh = openInterestDayHigh;
	}

	public Date getTickTimeStamp() {
		return tickTimestamp;
	}

	public void setTickTimeStamp(Date tickTimeStamp) {
		this.tickTimestamp = tickTimeStamp;
	}

	public double getTotalBuyQuantity() {
		return totalBuyQuantity;
	}

	public void setTotalBuyQuantity(double totalBuyQuantity) {
		this.totalBuyQuantity = totalBuyQuantity;
	}

	public double getTotalSellQuantity() {
		return totalSellQuantity;
	}

	public void setTotalSellQuantity(double totalSellQuantity) {
		this.totalSellQuantity = totalSellQuantity;
	}

	public double getVolumeTradedToday() {
		return volumeTradedToday;
	}

	public void setVolumeTradedToday(double volumeTradedToday) {
		this.volumeTradedToday = volumeTradedToday;
	}

	public boolean isTradable() {
		return tradable;
	}

	public void setTradable(boolean isTradable) {
		this.tradable = isTradable;
	}
	
	public Map<String,ArrayList<ZerodhaTickDepth>> getDepth() {
		return depth;
	}

	public void setDepth(Map<String,ArrayList<ZerodhaTickDepth>> depth) {
		this.depth = depth;
	}

	public void computeDepthStats() {
		// TODO Auto-generated method stub
		List buyList = depth.get("buy");
		List sellList = depth.get("sell");
		
		computeAvgDepthValue(buyList,"buy");
		computeAvgDepthValue(sellList,"sell");
		
		computeRangeDepthValue(buyList,"buy");
		computeRangeDepthValue(sellList,"sell");
		
	}

	private void computeRangeDepthValue(List<ZerodhaTickDepth> depthList, String depthType) {
		
		// TODO Auto-generated method stub
		
		if(depthType=="buy") {

			this.setDepth_buy_price_range_percent((depthList.get(0).price - depthList.get(depthList.size()-1).price)/this.lastTradedPrice);
			
		}else {
			this.setDepth_sell_price_range_percent((depthList.get(0).price - depthList.get(depthList.size()-1).price)/this.lastTradedPrice);
		}
		
	}


	private void computeAvgDepthValue(List<ZerodhaTickDepth> depthList, String depthType) {
		
		
		// TODO Auto-generated method stub
		double avgPrice=0;
		double avgOrder=0;
		double avgQuantity=0;
				
		Iterator iter = depthList.iterator();
		
		while(iter.hasNext()){
			
			ZerodhaTickDepth iterDepth = (ZerodhaTickDepth) iter.next();
			
			avgPrice = avgPrice + iterDepth.price;
			avgOrder = avgOrder + iterDepth.orders;
			avgQuantity = avgQuantity + iterDepth.quantity;
			
		}
		
		if(depthType=="buy") {
			this.setDepth_buy_orders_avg(avgOrder/depthList.size());
			this.setDepth_buy_price_avg(avgPrice/depthList.size());
			this.setDepth_buy_quantity_avg(avgQuantity/depthList.size());
		}else {
			this.setDepth_sell_orders_avg(avgOrder/depthList.size());
			this.setDepth_sell_price_avg(avgPrice/depthList.size());
			this.setDepth_sell_quantity_avg(avgQuantity/depthList.size());
		}
	}

	public double getDepth_buy_orders_avg() {
		return depth_buy_orders_avg;
	}

	public void setDepth_buy_orders_avg(double depth_buy_orders_avg) {
		this.depth_buy_orders_avg = depth_buy_orders_avg;
	}

	public double getDepth_buy_quantity_avg() {
		return depth_buy_quantity_avg;
	}

	public void setDepth_buy_quantity_avg(double depth_buy_quantity_avg) {
		this.depth_buy_quantity_avg = depth_buy_quantity_avg;
	}

	public double getDepth_buy_price_avg() {
		return depth_buy_price_avg;
	}

	public void setDepth_buy_price_avg(double depth_buy_price_avg) {
		this.depth_buy_price_avg = depth_buy_price_avg;
	}

	public double getDepth_sell_orders_avg() {
		return depth_sell_orders_avg;
	}

	public void setDepth_sell_orders_avg(double depth_sell_orders_avg) {
		this.depth_sell_orders_avg = depth_sell_orders_avg;
	}

	public double getDepth_sell_quantity_avg() {
		return depth_sell_quantity_avg;
	}

	public void setDepth_sell_quantity_avg(double depth_sell_quantity_avg) {
		this.depth_sell_quantity_avg = depth_sell_quantity_avg;
	}

	public double getDepth_sell_price_avg() {
		return depth_sell_price_avg;
	}

	public void setDepth_sell_price_avg(double depth_sell_price_avg) {
		this.depth_sell_price_avg = depth_sell_price_avg;
	}

	public double getDepth_buy_price_range_percent() {
		return depth_buy_price_range_percent;
	}

	public void setDepth_buy_price_range_percent(double depth_buy_price_range_percent) {
		this.depth_buy_price_range_percent = depth_buy_price_range_percent;
	}

	public double getDepth_sell_price_range_percent() {
		return depth_sell_price_range_percent;
	}

	public void setDepth_sell_price_range_percent(double depth_sell_price_range_percent) {
		this.depth_sell_price_range_percent = depth_sell_price_range_percent;
	}
	

}
