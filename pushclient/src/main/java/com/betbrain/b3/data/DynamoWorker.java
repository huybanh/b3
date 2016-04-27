package com.betbrain.b3.data;

import java.util.Arrays;

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
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

public class DynamoWorker {
	
	private static String HASH = "hash";
	private static String RANGE = "range";
	
	private static String[] BUNDLEIDS = {"X", "Y", "Z", "T", "U"};
	
	public static final String BUNDLE_STATUS_CREATED = "CREATED";
	public static final String BUNDLE_STATUS_INITIALDUMP = "INITIALDUMP";
	public static final String BUNDLE_STATUS_PUSHING = "PUSHING";
	public static final String BUNDLE_STATUS_PUSHING_INTERRUPTED = "PUSHING_INTERRUPTED";
	public static final String BUNDLE_STATUS_DELETING = "DELETING";
	public static final String BUNDLE_STATUS_UNUSED = "UNUSED";
	
	private static final String BUNDLE_HASH = "BUNDLE";
	private static final String BUNDLE_RANGE_CURRENT = "CURRENT";
	private static final String BUNDLE_CELL_ID = "ID";
	private static final String BUNDLE_CELL_STATUS = "STATUS";
	
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

	public static String getBundleIdCurrent() {
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, BUNDLE_RANGE_CURRENT)
				.withAttributesToGet(BUNDLE_CELL_ID);
		Item item = entityTable.getItem(spec);
		if (item == null) {
			return BUNDLEIDS[0];
		}		

		return item.getString(BUNDLE_CELL_ID);
	}
	
	public static String allocateBundleForInitialDump() {
		String currentId = getBundleIdCurrent();
		int currentIndex = Arrays.asList(BUNDLEIDS).indexOf(currentId);
		if (currentIndex < 0) {
			throw new RuntimeException("Unknown current bundle id: " + currentId);
		}
		int proposedIndex = currentIndex;
		int count = 0;
		String availBundleId;
		while (true) {
			proposedIndex++;
			proposedIndex = proposedIndex < BUNDLEIDS.length ? proposedIndex : 0; //round robin
			String status = getBundleStatus(BUNDLEIDS[proposedIndex]);
			if (status == null || status.equals(BUNDLE_STATUS_UNUSED)) {
				availBundleId = BUNDLEIDS[proposedIndex];
				break;
			}
			count++;
			if (count >= BUNDLEIDS.length) {
				throw new RuntimeException("No available bundle");
			}
		}
		
		UpdateItemSpec us = new UpdateItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, availBundleId)
				.addAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_STATUS).put(BUNDLE_STATUS_INITIALDUMP));
		entityTable.updateItem(us);
		return availBundleId;
	}
	
	private static String getBundleStatus(String bundleId) {
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, bundleId)
				.withAttributesToGet(BUNDLE_CELL_STATUS);
		Item item = entityTable.getItem(spec);
		if (item == null) {
			return null;
		}
		return item.getString(BUNDLE_CELL_STATUS);
	}
	
	public static void get(int bundleId, String hash, String range) {
		
	}
	
	public static void put(String bundleId, B3Update update) {
		/*Item item = new Item().withPrimaryKey(HASH, hash, RANGE, range);
		if (cell != null) {
			item = item.withString(cell, value);
		}*/

		Table dynaTable = getTable(update.table);
		UpdateItemSpec us = new UpdateItemSpec().withPrimaryKey(
				HASH, bundleId + update.key.getHashKey(), RANGE, update.key.getRangeKey());
		if (update.cells != null) {
			for (B3Cell<?> c : update.cells) {
				us = us.addAttributeUpdate(new AttributeUpdate(c.columnName).put(c.value));
			}
		}

		dynaTable.updateItem(us);
		/*int colCount = update.cells == null ? 0 : update.cells.length;
		System.out.println(update.table.name + ": " + bundleId + update.key.getRangeKey() + "@" + 
				update.key.getRangeKey() + ", cols: " + colCount);*/
	}

	public static Item get(B3Table b3table, String hashKey, String rangeKey) {
		Table table = getTable(b3table);
		return table.getItem(HASH, hashKey, RANGE, rangeKey);
	}
	
	public static ItemCollection<QueryOutcome> query(B3Table b3table, String hashKey) {
		
		Table table = getTable(b3table);
		return table.query(HASH, hashKey);
		/*ScanRequest scanRequest = new ScanRequest()
		        .withTableName(table.getTableName())
		        .withLimit(10)
		        .addExclusiveStartKeyEntry(HASH, new AttributeValue(hashKey));
		scanRequest.addExclusiveStartKeyEntry(RANGE, new AttributeValue(""));
		ScanResult result = dynaClient.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()){
	        for (Entry<String, AttributeValue> x : item.entrySet()) {
	        	System.out.println(x.getKey() + ": " + x.getValue());
	        }
	    }*/
		
		/*ScanSpec spec = new ScanSpec().withExclusiveStartKey(HASH, hashKey, RANGE, "a");
		ItemCollection<ScanOutcome> coll = table.scan(spec);
		IteratorSupport<Item, ScanOutcome> it = coll.iterator();
		while (it.hasNext()) {
			Item item = it.next();
		}*/
		
		/*ItemCollection<QueryOutcome> coll = table.query(HASH, hashKey);
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		while (it.hasNext()) {
			Item item = it.next();
			
		}*/
	}

	public static void main(String[] args) {
		initialize();
		/*TableCollection<ListTablesResult> x = dynamoDB.listTables();
		System.out.println(x);
		for (Table i : x) {
			System.out.println(i);
		}
		Table table = dynamoDB.getTable("fbook");
		System.out.println(table);
		table.deleteItem(HASH, "o3641", RANGE, "p_1005123616170333/1005123616170333_1094715557211138");*/
		System.out.println(allocateBundleForInitialDump());
	}
}
