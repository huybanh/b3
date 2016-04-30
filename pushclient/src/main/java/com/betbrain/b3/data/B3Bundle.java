package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

class B3Bundle {

	private final String id;
	
	Table offerTable;
	Table eventTable;
	Table eventInfoTable;
	Table outcomeTable;
	Table lookupTable;
	Table linkTable;
	Table entityTable;
	Table sepcTable;
	
	static B3Bundle workingBundle;
	
	private B3Bundle(String id) {
		this.id = id;
	}
	
	static void initWorkingBundle(DynamoDB dynamoDB, String id) {
		workingBundle = new B3Bundle(id);
		workingBundle.init(dynamoDB);
	}
	
	static String getWorkingBundleId() {
		return workingBundle.id;
	}
	
	private void init(DynamoDB dynamoDB) {

		offerTable = dynamoDB.getTable(id + "offer");
		eventTable = dynamoDB.getTable(id + "event");
		eventInfoTable = dynamoDB.getTable(id + "event_info");
		outcomeTable = dynamoDB.getTable(id + "outcome");
		lookupTable = dynamoDB.getTable(id + "lookup");
		linkTable = dynamoDB.getTable(id + "link");
		entityTable = dynamoDB.getTable(id + "entity");
		sepcTable = dynamoDB.getTable(id + "sepc");
	}
	
	Table getTable(B3Table b3table) {

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
	
	static void createTables(DynamoDB dynamoDB, String id) {

		createTable(dynamoDB, id, "offer", 5, 5, true);
		createTable(dynamoDB, id, "event", 5, 5, true);
		createTable(dynamoDB, id, "event_info", 5, 5, true);
		createTable(dynamoDB, id, "outcome", 5, 5, true);
		createTable(dynamoDB, id, "lookup", 5, 5, true);
		createTable(dynamoDB, id, "link", 5, 5, true);
		createTable(dynamoDB, id, "entity", 5, 5, false);
		createTable(dynamoDB, id, "sepc", 5, 5, true);
	}
    
    private static void createTable(DynamoDB dynamoDB, String prefix,
        String tableName, long readCapacityUnits, long writeCapacityUnits, boolean withRangeKey) {
        
        try {
            System.out.println("Creating table " + tableName);
            
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                .withAttributeName(DynamoWorker.HASH)
                .withKeyType(KeyType.HASH)); //Partition key
            
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(DynamoWorker.HASH)
                .withAttributeType("S"));

            if (withRangeKey){
                keySchema.add(new KeySchemaElement()
                    .withAttributeName(DynamoWorker.RANGE)
                    .withKeyType(KeyType.RANGE)); //Sort key
                attributeDefinitions.add(new AttributeDefinition()
                      .withAttributeName(DynamoWorker.RANGE)
                      .withAttributeType("S"));
            }

            Table table = dynamoDB.createTable(tableName, 
                keySchema,
                attributeDefinitions, 
                new ProvisionedThroughput()
                    .withReadCapacityUnits(readCapacityUnits)
                    .withWriteCapacityUnits(writeCapacityUnits));
            System.out.println("Waiting for " + tableName
                + " to be created...this may take a while...");
            table.waitForActive();
       
            
        } catch (Exception e) {
            System.err.println("Failed to create table " + tableName);
            e.printStackTrace(System.err);
        }
    }
}
