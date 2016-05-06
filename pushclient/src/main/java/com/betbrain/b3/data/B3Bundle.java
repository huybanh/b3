package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

class B3Bundle {
	
    private final Logger logger = Logger.getLogger(this.getClass());

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
		logger.info("Working bundle: " + id);
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

		Table[] tables = new Table[8];
		int capaHigh = 1000;
		//int capaLow = 200;
		int i = 0;
		tables[i++] = createTable(dynamoDB, id, "offer", 1, 300, true);
		tables[i++] = createTable(dynamoDB, id, "event", 1, 50, true);
		tables[i++] = createTable(dynamoDB, id, "event_info", 1, 1, true);
		tables[i++] = createTable(dynamoDB, id, "outcome", 1, 800, true);
		tables[i++] = createTable(dynamoDB, id, "lookup", 1, capaHigh, true);
		tables[i++] = createTable(dynamoDB, id, "link", 1, capaHigh, true);
		tables[i++] = createTable(dynamoDB, id, "entity", 1, 150, false);
		tables[i++] = createTable(dynamoDB, id, "sepc", 1, 300/*capaLow*/, true);
		for (Table t : tables) {
			try {
		        System.out.println("Waiting for " + t.getTableName() + " to be created...this may take a while...");
				t.waitForActive();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
    
    private static Table createTable(DynamoDB dynamoDB, String prefix,
        String tableName, long readCapacityUnits, long writeCapacityUnits, boolean withRangeKey) {
        
        System.out.println("Creating table " + prefix + tableName);
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

        Table table = dynamoDB.createTable(prefix + tableName, 
            keySchema,
            attributeDefinitions, 
            new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacityUnits)
                .withWriteCapacityUnits(writeCapacityUnits));
        //table.waitForActive();
        return table;
    }
	
	void deleteTables(DynamoDB dynamoDB) {

		Table[] tables = new Table[] {offerTable, eventTable, eventInfoTable, 
				outcomeTable, lookupTable, linkTable, entityTable, sepcTable};
		for (Table t : tables) {
			System.out.println("Deleting table " + t.getTableName());
			t.delete();
		}
		
		for (Table t : tables) {
			try {
				t.waitForDelete();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	void ceaseThroughPuts() {
		
		Table[] tables = new Table[] {offerTable, eventTable, eventInfoTable, 
				outcomeTable, lookupTable, linkTable, entityTable, sepcTable};
		for (Table t : tables) {
			System.out.println("Ceasing throughput table " + t.getTableName());
			ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				    .withReadCapacityUnits(1L)
				    .withWriteCapacityUnits(1L);

			t.updateTable(provisionedThroughput);
		}
		
		for (Table t : tables) {
			try {
				t.waitForActive();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		
	}
}
