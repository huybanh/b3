package com.betbrain.b3.data;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

public class DynamoWorker {
	
	private static DynamoDB dynamoDB;
	public static Table offerTable;
	public static Table eventTable;
	public static Table lookupTable;
	public static Table linkTable;
	public static Table entityTable;
	
	public static void initialize() {

		AmazonDynamoDBClient dynaClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dynaClient.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
		dynamoDB = new DynamoDB(dynaClient);
		offerTable = dynamoDB.getTable("offer");
		eventTable = dynamoDB.getTable("event");
		lookupTable = dynamoDB.getTable("lookup");
		linkTable = dynamoDB.getTable("link");
		entityTable = dynamoDB.getTable("entity");
	}
	
	public static void put(B3Update update) {

		Table dynaTable;
		if (update.table == B3Table.BettingOffer) {
			dynaTable = offerTable;
		} else if (update.table == B3Table.Event) {
			dynaTable = eventTable;
		} else if (update.table == B3Table.Lookup) {
			dynaTable = lookupTable;
		} else if (update.table == B3Table.Link) {
			dynaTable = linkTable;
		} else if (update.table == B3Table.Entity) {
			dynaTable = entityTable;
		} else {
			throw new RuntimeException("Unmapped table: " + update.table);
		}
		/*Item item = new Item().withPrimaryKey("hash", hash, "range", range);
		if (cell != null) {
			item = item.withString(cell, value);
		}*/
		
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
}
