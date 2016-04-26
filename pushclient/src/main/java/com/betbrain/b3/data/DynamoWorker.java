package com.betbrain.b3.data;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

public class DynamoWorker {
	
	private  static AmazonDynamoDBClient dynaClient;
	private static DynamoDB dynamoDB;
	
	public static Table offerTable;
	public static Table eventTable;
	public static Table eventInfoTable;
	public static Table lookupTable;
	public static Table linkTable;
	public static Table entityTable;
	
	public static void initialize() {

		dynaClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dynaClient.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
		dynamoDB = new DynamoDB(dynaClient);
		offerTable = dynamoDB.getTable("offer");
		eventTable = dynamoDB.getTable("event");
		eventInfoTable = dynamoDB.getTable("event_info");
		lookupTable = dynamoDB.getTable("lookup");
		linkTable = dynamoDB.getTable("link");
		entityTable = dynamoDB.getTable("entity");
	}
	
	private static Table getTable(B3Table b3table) {

		if (b3table == B3Table.BettingOffer) {
			return offerTable;
		} else if (b3table == B3Table.Event) {
			return eventTable;
		} else if (b3table == B3Table.EventInfo) {
			return eventInfoTable;
		} else if (b3table == B3Table.Lookup) {
			return lookupTable;
		} else if (b3table == B3Table.Link) {
			return linkTable;
		} else if (b3table == B3Table.Entity) {
			return entityTable;
		} else {
			throw new RuntimeException("Unmapped table: " + b3table);
		}
	}
	
	public static void put(B3Update update) {
		/*Item item = new Item().withPrimaryKey("hash", hash, "range", range);
		if (cell != null) {
			item = item.withString(cell, value);
		}*/

		Table dynaTable = getTable(update.table);
		UpdateItemSpec us = new UpdateItemSpec().withPrimaryKey(
				"hash", update.key.getHashKey(), "range", update.key.getRangeKey());
		if (update.cells != null) {
			for (B3Cell<?> c : update.cells) {
				us = us.addAttributeUpdate(new AttributeUpdate(c.columnName).put(c.value));
			}
		}
		
		int colCount = update.cells == null ? 0 : update.cells.length;
		System.out.println(update.table.name + ": " + update.key.getRangeKey() + "@" + 
				update.key.getRangeKey() + ", cols: " + colCount);
		dynaTable.updateItem(us);
	}
	
	public static ItemCollection<QueryOutcome> query(B3Table b3table, String hashKey) {
		
		Table table = getTable(b3table);
		return table.query("hash", hashKey);
		/*ScanRequest scanRequest = new ScanRequest()
		        .withTableName(table.getTableName())
		        .withLimit(10)
		        .addExclusiveStartKeyEntry("hash", new AttributeValue(hashKey));
		scanRequest.addExclusiveStartKeyEntry("range", new AttributeValue(""));
		ScanResult result = dynaClient.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()){
	        for (Entry<String, AttributeValue> x : item.entrySet()) {
	        	System.out.println(x.getKey() + ": " + x.getValue());
	        }
	    }*/
		
		/*ScanSpec spec = new ScanSpec().withExclusiveStartKey("hash", hashKey, "range", "a");
		ItemCollection<ScanOutcome> coll = table.scan(spec);
		IteratorSupport<Item, ScanOutcome> it = coll.iterator();
		while (it.hasNext()) {
			Item item = it.next();
		}*/
		
		/*ItemCollection<QueryOutcome> coll = table.query("hash", hashKey);
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		while (it.hasNext()) {
			Item item = it.next();
			
		}*/
	}

	public static void main(String[] args) {
		initialize();
		TableCollection<ListTablesResult> x = dynamoDB.listTables();
		System.out.println(x);
		for (Table i : x) {
			System.out.println(i);
		}
		Table table = dynamoDB.getTable("fbook");
		System.out.println(table);
		table.deleteItem("hash", "o3641", "range", "p_1005123616170333/1005123616170333_1094715557211138");
	}

	public static Item get(B3Table b3table, String hashKey, String rangeKey) {
		Table table = getTable(b3table);
		return table.getItem("hash", hashKey, "range", rangeKey);
	}
}
