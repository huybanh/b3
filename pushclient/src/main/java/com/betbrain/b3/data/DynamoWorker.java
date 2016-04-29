package com.betbrain.b3.data;

import java.util.Arrays;
import java.util.function.Consumer;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

public class DynamoWorker {
	
	private static String HASH = "hash";
	private static String RANGE = "range";
	
	private static String[] BUNDLEIDS = {"X", "Y", "Z", "T", "U"};
	
	public static final String BUNDLE_STATUS_INITIALPUT = "INITIAL-PUT";
	public static final String BUNDLE_STATUS_DEPLOYWAIT= "DEPLOY-WAIT";
	public static final String BUNDLE_STATUS_DEPLOYING = "DEPLOYING";
	public static final String BUNDLE_STATUS_PUSHING = "PUSHING";
	public static final String BUNDLE_STATUS_CEASED = "CEASED";
	public static final String BUNDLE_STATUS_DELETING = "DELETING";
	public static final String BUNDLE_STATUS_UNUSED = "UNUSED";

	public static final String BUNDLE_PUSHSTATUS_ONGOING = "ONGOING";
	public static final String BUNDLE_PUSHSTATUS_INTERRUPTED = "INTERRUPTED";
	
	private static final String BUNDLE_HASH = "BUNDLE";
	private static final String BUNDLE_RANGE_CURRENT = "CURRENT";
	private static final String BUNDLE_CELL_ID = "ID";
	private static final String BUNDLE_CELL_STATUS = "STATUS";
	
	public static final String SEPC_CELLNAME_JSON = "JSON";
	public static final String SEPC_INITIAL = "I";
	public static final String SEPC_CHANGEBATCH = "B";
	
	private  static AmazonDynamoDBClient dynaClient;
	private static DynamoDB dynamoDB;
	
	private static Table offerTable;
	private static Table eventTable;
	private static Table eventInfoTable;
	private static Table outcomeTable;
	private static Table lookupTable;
	private static Table linkTable;
	private static Table entityTable;
	private static Table sepcTable;
	private static Table settingTable;
	
	public static void initialize() {

		dynaClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dynaClient.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
		dynamoDB = new DynamoDB(dynaClient);
		offerTable = dynamoDB.getTable("offer");
		eventTable = dynamoDB.getTable("event");
		eventInfoTable = dynamoDB.getTable("event_info");
		outcomeTable = dynamoDB.getTable("outcome");
		lookupTable = dynamoDB.getTable("lookup");
		linkTable = dynamoDB.getTable("link");
		entityTable = dynamoDB.getTable("entity2");
		sepcTable = dynamoDB.getTable("sepc");
		settingTable = dynamoDB.getTable("setting");
	}
	
	private static Table getTable(B3Table b3table) {

		if (b3table == B3Table.BettingOffer) {
			return offerTable;
		} else if (b3table == B3Table.Event) {
			return eventTable;
		} else if (b3table == B3Table.EventInfo) {
			return eventInfoTable;
		} else if (b3table == B3Table.Outcome) {
			return outcomeTable;
		} else if (b3table == B3Table.Lookup) {
			return lookupTable;
		} else if (b3table == B3Table.Link) {
			return linkTable;
		} else if (b3table == B3Table.Entity) {
			return entityTable;
		} else if (b3table == B3Table.SEPC) {
			return sepcTable;
		} else {
			throw new RuntimeException("Unmapped table: " + b3table);
		}
	}

	public static B3Bundle getBundleCurrent() {
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, BUNDLE_RANGE_CURRENT)
				.withAttributesToGet(BUNDLE_CELL_ID);
		Item item = settingTable.getItem(spec);
		String id;
		if (item == null) {
			id = BUNDLEIDS[0];
		} else {
			id = item.getString(BUNDLE_CELL_ID);
		}
		return new B3Bundle(id);
	}
	
	public static void setBundleCurrent(String id) {
		
		UpdateItemSpec us = new UpdateItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, BUNDLE_RANGE_CURRENT)
				.addAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_ID).put(id));
		settingTable.updateItem(us);
	}
	
	public static B3Bundle getBundleUnused(String newStatus) {
		String currentId = getBundleCurrent().id;
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
		
		if (newStatus != null) {
			UpdateItemSpec us = new UpdateItemSpec()
					.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, availBundleId)
					.addAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_STATUS).put(newStatus));
			settingTable.updateItem(us);
		}
		return new B3Bundle(availBundleId);
	}
	
	public static B3Bundle getBundleByStatus(String requiredStatus) {
		int proposedIndex = 0;
		int count = 0;
		String foundBundleId;
		while (true) {
			proposedIndex++;
			proposedIndex = proposedIndex < BUNDLEIDS.length ? proposedIndex : 0; //round robin
			String oneStatus = getBundleStatus(BUNDLEIDS[proposedIndex]);
			if (oneStatus != null && oneStatus.equals(requiredStatus)) {
				foundBundleId = BUNDLEIDS[proposedIndex];
				break;
			}
			count++;
			if (count >= BUNDLEIDS.length) {
				//throw new RuntimeException("Found no bundles with status of " + requiredStatus);
				return null;
			}
		}
		
		return new B3Bundle(foundBundleId);
	}
	
	private static String getBundleStatus(String bundleId) {
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, bundleId)
				.withAttributesToGet(BUNDLE_CELL_STATUS);
		Item item = settingTable.getItem(spec);
		if (item == null) {
			return null;
		}
		return item.getString(BUNDLE_CELL_STATUS);
	}
	
	public static void setBundleStatus(B3Bundle bundle, String status) {
		UpdateItemSpec spec = new UpdateItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, bundle.id)
				.withAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_STATUS).put(status));
		settingTable.updateItem(spec);
	}
	
	public static void put(B3Bundle bundle, B3Update update) {
		/*Table dynaTable = getTable(update.table);
		UpdateItemSpec us = new UpdateItemSpec().withPrimaryKey(
				HASH, bundleId + update.key.getHashKey(), RANGE, update.key.getRangeKey());
		if (update.cells != null) {
			for (B3Cell<?> c : update.cells) {
				us = us.addAttributeUpdate(new AttributeUpdate(c.columnName).put(c.value));
			}
		}*/
		
		Table dynaTable = getTable(update.table);
		String rangeKey = update.key.getRangeKey();
		Item item ;
		if (rangeKey != null) {
			item = new Item().withPrimaryKey(HASH, bundle.id + update.key.getHashKey(), RANGE, update.key.getRangeKey());
		} else {
			item = new Item().withPrimaryKey(HASH, bundle.id + update.key.getHashKey());
		}
		if (update.cells != null) {
			for (B3Cell<?> c : update.cells) {
				if (c instanceof B3CellString) {
					item = item.withString(c.columnName, ((B3CellString) c).value);
				} else if (c instanceof B3CellLong) {
					item = item.withLong(c.columnName, ((B3CellLong) c).value);
				} else if (c instanceof B3CellInt) {
					item = item.withInt(c.columnName, ((B3CellInt) c).value);
				} else {
					throw new RuntimeException("Unknown B3Cell: " + c);
				}
			}
		}

		dynaTable.putItem(item);
		//System.out.println(update + ": " + update.toString().length());
	}
	
	public static void putSepc(B3Bundle bundle, String hashKey, String rangeKey, String[]... nameValuePairs ) {
		
		Item item = new Item().withPrimaryKey(HASH, bundle.id + hashKey, RANGE, rangeKey);
		if (nameValuePairs != null) {
			for (String[] onePair : nameValuePairs) {
				item = item.withString(onePair[0], onePair[1]);
			}
		}
		sepcTable.putItem(item);
		//System.out.println("SEPC: " + bundle.id + hash + "@" + value);
	}

	public static void updatex(String bundleId, B3Update update) {
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

	public static Item get(B3Table b3table, B3Bundle bundle, String hashKey, String rangeKey) {
		
		Table table = getTable(b3table);
		if (rangeKey == null) {
			System.out.println("GET " + table.getTableName() + ": " + bundle.id + hashKey);
			return table.getItem(HASH, bundle.id + hashKey);
		} else {
			System.out.println("GET " + table.getTableName() + ": " + bundle.id + hashKey + "@" + rangeKey);
			return table.getItem(HASH, bundle.id + hashKey, RANGE, rangeKey);
		}
	}
	
	private static void deleteBundle(B3Table b3table, final String bundleId) {
		final Table table = getTable(b3table);
		ScanSpec spec = new ScanSpec().withAttributesToGet(HASH, RANGE);
		ItemCollection<ScanOutcome> coll = table.scan(spec);
		coll.forEach(new Consumer<Item>() {

			public void accept(Item t) {
				String hashKey = t.getString(HASH);
				if (!hashKey.startsWith(bundleId)) {
					return;
				}
				System.out.println(hashKey);
				table.deleteItem(HASH, hashKey, RANGE, t.getString(RANGE));
			}
		});
		//table.deleteItem(HASH, hashKey);
	}
	
	public static ItemCollection<QueryOutcome> query(B3Bundle bundle, B3Table b3table, String hashKey) {
		
		Table table = getTable(b3table);
		System.out.println("QUERY " + table.getTableName() + ": hash=" + bundle.id + hashKey);
		return table.query(HASH, bundle.id + hashKey);
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
		
		//setBundleCurrent("Y");
		//System.out.println(getBundleUnused(null).id);
		
		//deleteBundle(B3Table.BettingOffer, "Y");
		System.out.println(getBundleByStatus(BUNDLE_STATUS_DEPLOYWAIT).id);
	}
}