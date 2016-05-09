package com.betbrain.b3.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.betbrain.b3.pushclient.JsonMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DynamoWorker {
	
	private static Logger logger = Logger.getLogger(DynamoWorker.class);
	
	static String HASH = "HASH";
	public static String RANGE = "RANGE";
	
	private static String[] BUNDLEIDS = {"x", "y", "z", "t", "u"};
	
	public static final String BUNDLE_STATUS_INITIALPUT = "INITIAL-PUT";
	public static final String BUNDLE_STATUS_DEPLOYWAIT= "DEPLOY-WAIT";
	public static final String BUNDLE_STATUS_DEPLOYING = "DEPLOYING";
	public static final String BUNDLE_STATUS_PUSH_WAIT = "PUSH-WAIT";
	public static final String BUNDLE_STATUS_PUSHING = "PUSHING";
	public static final String BUNDLE_STATUS_CEASED = "CEASED";
	//public static final String BUNDLE_STATUS_GARBAGE = "GARBAGE";
	public static final String BUNDLE_STATUS_DELETEWAIT = "DELETE-WAIT";
	public static final String BUNDLE_STATUS_NOTEXIST = "NOT-EXIST";
	public static final String BUNDLE_STATUS_EMPTY = "EMPTY";
	//public static final String BUNDLE_STATUS_UNUSED = "UNUSED";

	public static final String BUNDLE_PUSHSTATUS_ONGOING = "ONGOING";
	public static final String BUNDLE_PUSHSTATUS_INTERRUPTED = "INTERRUPTED";
	
	private static final String BUNDLE_HASH = "BUNDLE";
	private static final String BUNDLE_RANGE_CURRENT = "CURRENT";
	private static final String BUNDLE_CELL_ID = "ID";
	private static final String BUNDLE_CELL_STATUS = "STATUS";
	
	private static final String BUNDLE_ERROR = "ERROR";
	
	public static final String BUNDLE_CELL_PUSHSTATUS = "PUSH_STATUS";
	public static final String BUNDLE_CELL_LASTBATCH_RECEIVED_ID = "LAST_RECEIVED_ID";
	public static final String BUNDLE_CELL_LASTBATCH_RECEIVED_TIMESTAMP = "LAST_RECEIVED_TIMESTAMP";

	public static final String BUNDLE_CELL_DEPLOYSTATUS = "DEPLOY_STATUS";
	public static final String BUNDLE_CELL_LASTBATCH_DEPLOYED_ID = "LAST_DEPLOYED_ID";
	public static final String BUNDLE_CELL_LASTBATCH_DEPLOYED_TIMESTAMP = "LAST_DEPLOYED_TIMESTAMP";
	
	public static final String SEPC_INITIAL = "I";
	public static final String SEPC_CHANGEBATCH = "B";
	public static final String SEPC_CELLNAME_JSON = "JSON";
	public static final String SEPC_CELLNAME_CREATETIME = "CREATE_TIME";
	//public static final String SEPC_CELLNAME_CHANGES = "CHANGES";
	
	private  static AmazonDynamoDBClient dynaClient;
	private static DynamoDB dynamoDB;
	
	static Table settingTable;
	
	private static void initialize() {

		EntitySpec2.initialize();
		dynaClient = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
		dynaClient.setRegion(Region.getRegion(Regions.AP_SOUTHEAST_1));
		dynamoDB = new DynamoDB(dynaClient);
		settingTable = dynamoDB.getTable("setting");
	}
	
	public static void createTables() {
		initialize();
		String availBundleId = findBundleIdByStatus(BUNDLE_STATUS_NOTEXIST, true);
		B3Bundle.createTables(dynamoDB, availBundleId);
		setBundleStatus(availBundleId, BUNDLE_STATUS_EMPTY);
	}
	
	public static void deleteTables() {
		initBundleByStatus(BUNDLE_STATUS_DELETEWAIT);
		B3Bundle.workingBundle.deleteTables(dynamoDB);
		setWorkingBundleStatus(BUNDLE_STATUS_NOTEXIST);
		
		QuerySpec spec = new QuerySpec().withHashKey(HASH, BUNDLE_ERROR)
				.withRangeKeyCondition(new RangeKeyCondition(RANGE).beginsWith(B3Bundle.getWorkingBundleId()));
		ItemCollection<QueryOutcome> coll = settingTable.query(spec);
		if (coll == null) {
			return;
		}
		IteratorSupport<Item, QueryOutcome> it = coll.iterator();
		while (it.hasNext()) {
			Item item = it.next();
			delete(B3Table.Setting, BUNDLE_ERROR, item.getString(RANGE));
		}
	}

	public static void initBundleCurrent() {
		initialize();
		String currentBundleId = getCurrentBundleId();
		if (currentBundleId == null) {
			throw new RuntimeException("No current bundle found");
		}
		B3Bundle.initWorkingBundle(dynamoDB, getCurrentBundleId());
	}
	
	private static String getCurrentBundleId() {
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, BUNDLE_RANGE_CURRENT)
				.withAttributesToGet(BUNDLE_CELL_ID);
		Item item = settingTable.getItem(spec);
		if (item == null) {
			//return BUNDLEIDS[0];
			return null;
		}

		return item.getString(BUNDLE_CELL_ID);
	}
	
	public static void activateWorkingBundle() {
		
		UpdateItemSpec us = new UpdateItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, BUNDLE_RANGE_CURRENT)
				.addAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_ID).put(B3Bundle.getWorkingBundleId()));
		settingTable.updateItem(us);
	}
	
	/*public static void initBundleEmpty() {

		initialize();
		String availBundleId = findBundleIdByStatus(BUNDLE_STATUS_EMPTY, false);
		B3Bundle.initWorkingBundle(dynamoDB, availBundleId);
	}*/
	
	private static String findBundleIdByStatus(String requiredStatus, boolean takeNull) {
		String currentId = getCurrentBundleId();
		if (currentId == null) {
			currentId = BUNDLEIDS[0];
		}
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
			if ((takeNull && status == null) || status.equals(requiredStatus)) {
				availBundleId = BUNDLEIDS[proposedIndex];
				break;
			}
			count++;
			if (count >= BUNDLEIDS.length) {
				throw new RuntimeException("No available bundle");
			}
		}
		return availBundleId;
	}
	
	public static boolean initBundleByStatus(String requiredStatus) {
		initialize();
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
				//return null;
				logger.info("Found no bundles with status " + requiredStatus);
				return false;
			}
		}
		
		B3Bundle.initWorkingBundle(dynamoDB, foundBundleId);
		return true;
	}
	
	private static String getBundleStatus(String bundleId) {
		
		System.out.println("Reading bundle status: " + bundleId);
		GetItemSpec spec = new GetItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, bundleId)
				.withAttributesToGet(BUNDLE_CELL_STATUS);
		Item item = settingTable.getItem(spec);
		if (item == null) {
			return null;
		}
		return item.getString(BUNDLE_CELL_STATUS);
	}
	
	public static void setWorkingBundleStatus(String status) {
		setBundleStatus(B3Bundle.getWorkingBundleId(), status);
	}
	
	private static void setBundleStatus(String bundleId, String status) {
		UpdateItemSpec spec = new UpdateItemSpec()
				.withPrimaryKey(HASH, BUNDLE_HASH, RANGE, bundleId)
				.withAttributeUpdate(new AttributeUpdate(BUNDLE_CELL_STATUS).put(status));
		settingTable.updateItem(spec);
	}
	
	public static boolean readOnly = false;
	//public static boolean readOnly = true;

	/*public static void put(B3Update update) {
		put(update.table, update.key.getHashKey(), update.key.getRangeKey(), update.cells);
	}*/
	
	private static BufferedWriter outOffer;
	private static BufferedWriter outOutcome;
	private static BufferedWriter outInfo;
	private static BufferedWriter outEvent;
	
	private static BufferedWriter outEntity;
	private static BufferedWriter outLink;
	private static BufferedWriter outLookup;
	
	private static BufferedReader inOffer;
	private static BufferedReader inOutcome;
	private static BufferedReader inInfo;
	private static BufferedReader inEvent;
	
	private static BufferedReader inEntity;
	private static BufferedReader inLink;
	private static BufferedReader inLookup;
	
	private static BufferedReader[] allReaders;
	
	private static B3Table[] tableByReader;
	private static Long[] pendTimes = new Long[] {null, null, null, null, null, null, null};
	private static long[] recordCount = new long[] {0, 0, 0, 0, 0, 0, 0};
	private static JsonObject[] pendPuts = new JsonObject[] {null, null, null, null, null, null, null};

	private static int readerIndex = 0;
	
	public static void openLocalWriters() {
		try {
			outOffer = new BufferedWriter(new FileWriter("T_" + B3Table.BettingOffer.name, false));
			outOutcome = new BufferedWriter(new FileWriter("T_" + B3Table.Outcome.name, false));
			outInfo = new BufferedWriter(new FileWriter("T_" + B3Table.EventInfo.name, false));
			outEvent = new BufferedWriter(new FileWriter("T_" + B3Table.Event.name, false));
			
			outEntity = new BufferedWriter(new FileWriter("T_" + B3Table.Entity.name, false));
			outLink = new BufferedWriter(new FileWriter("T_" + B3Table.Link.name, false));
			outLookup = new BufferedWriter(new FileWriter("T_" + B3Table.Lookup.name, false));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static void closeWriters() {
		try {
			outOffer.close();
			outOutcome.close();
			outInfo.close();
			outEvent.close();
			
			outEntity.close();
			outLink.close();
			outLookup.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static void openLocalReaders() {
		try {
			inOffer = new BufferedReader(new FileReader("T_" + B3Table.BettingOffer.name));
			inOutcome = new BufferedReader(new FileReader("T_" + B3Table.Outcome.name));
			inInfo = new BufferedReader(new FileReader("T_" + B3Table.EventInfo.name));
			inEvent = new BufferedReader(new FileReader("T_" + B3Table.Event.name));
			
			inEntity = new BufferedReader(new FileReader("T_" + B3Table.Entity.name));
			inLink = new BufferedReader(new FileReader("T_" + B3Table.Link.name));
			inLookup = new BufferedReader(new FileReader("T_" + B3Table.Lookup.name));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		allReaders = new BufferedReader[] {
				inOffer, inOutcome, inInfo, inEvent, inEntity, inLink, inLookup
		};
		tableByReader = new B3Table[] {
				B3Table.BettingOffer, B3Table.Outcome, B3Table.EventInfo, B3Table.Event,
				B3Table.Entity, B3Table.Link, B3Table.Lookup
		};
	}
	
	private static void closeReaders() {
		try {
			inOffer.close();
			inOutcome.close();
			inInfo.close();
			inEvent.close();
			
			inEntity.close();
			inLink.close();
			inLookup.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static BufferedWriter getWriter(B3Table b3table) {

		if (b3table == B3Table.BettingOffer) {
			return outOffer;
		} else if (b3table == B3Table.Event) {
			return outEvent;
		} else if (b3table == B3Table.EventInfo) {
			return outInfo;
		} else if (b3table == B3Table.Outcome) {
			return outOutcome;
		} else if (b3table == B3Table.Lookup) {
			return outLookup;
		} else if (b3table == B3Table.Link) {
			return outLink;
		} else if (b3table == B3Table.Entity) {
			return outEntity;
		} else {
			throw new RuntimeException("Unmapped table: " + b3table);
		}
	}
	
	public static void putAllFromLocal() {
		
		closeWriters();
		openLocalReaders();
		
		final LinkedList<Object> threadIds = new LinkedList<>();
		for (int i = 0; i < 100; i++) {
			final Object tid = new Object();
			threadIds.add(tid);
			new Thread() {
				
				//private JsonMapper mapper = new JsonMapper();
				private Gson gson = new Gson();
				
				public void run() {
					int printCount = 0;
					while (true) {
						BufferedReader reader = null;
						int readerChecked = 0;
						JsonObject putBean = null;
						String line = null;
						int thisReaderIndex;
						synchronized (DynamoWorker.class) {
							while (readerChecked < allReaders.length + 1) {
								readerChecked++;
								readerIndex++;
								if (readerIndex == allReaders.length) {
									readerIndex = 0;
								}
								if (pendTimes[readerIndex] != null && 
										System.currentTimeMillis() - pendTimes[readerIndex] < 100) {
									continue;
								}
								reader = allReaders[readerIndex];
								if (reader != null) {
									break;
								}
							}
							if (reader == null) {
								boolean foundReader = false;
								for (BufferedReader r : allReaders) {
									if (r != null) {
										foundReader = true;
										break;
									}
								}
								if (foundReader) {
									try {
										System.out.println(Thread.currentThread().getName() + 
												": All tables are busy, sleep 1000 ms");
										Thread.sleep(1000);
									} catch (InterruptedException e) {
									}
									continue;
								}
								System.out.println(Thread.currentThread().getName() + 
										": No more readers, stop thread");
								synchronized (threadIds) {
									threadIds.remove(tid);
									threadIds.notifyAll();
								}
								return;
							}
						
							//readerIndex pointing to a put
							thisReaderIndex = readerIndex;
							putBean = pendPuts[readerIndex];
							pendPuts[readerIndex] = null;
							if (putBean == null) {
								try {
									line = reader.readLine();
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
								if (line == null) {
									allReaders[readerIndex] = null;
									continue;
								}
							}
						}
						
						if (putBean == null) {
							//System.out.println(line);
							//putBean = (PutBean) mapper.deserialize(line);
							putBean = gson.fromJson(line, JsonObject.class);
						}
						
						//List<B3Cell<?>> cells = putBean.getB3Cells();
						JsonElement ele = putBean.get("b3Cells");
						B3Cell<?>[] cells = null;
						if (ele != null && !ele.isJsonNull()) {
							JsonArray arr = ele.getAsJsonArray();
							cells = new B3Cell<?>[arr.size()];
							for (int i = 0; i < arr.size(); i++) {
								JsonObject oneCell = arr.get(i).getAsJsonObject();
								String colName = oneCell.get("columnName").getAsString();
								String colValue = oneCell.get("value").getAsString();
								cells[i] = new B3CellString(colName, colValue);
							}
						}
						
						String range = null;
						ele = putBean.get("range");
						if (ele != null && !ele.isJsonNull()) {
							range = ele.getAsString();
						}
						boolean success = put(false, tableByReader[thisReaderIndex], 
								putBean.get("hash").getAsString(), 
								range,
								cells);
						
						synchronized (DynamoWorker.class) {
							if (!success) {
								pendPuts[thisReaderIndex] = putBean;
								pendTimes[thisReaderIndex] = System.currentTimeMillis();
							} else {
								recordCount[thisReaderIndex]++;
								printCount++;
								if (printCount == 5000) {
									printCount = 0;
									System.out.println(Thread.currentThread().getName() + 
											": Table " + tableByReader[readerIndex].name + 
											": records: " + recordCount[readerIndex]);
								}
							}
						}
					}
				}
			}.start();
		}
		
		synchronized (threadIds) {
			while (true) {
				if (threadIds.isEmpty()) {
					break;
				}
				try {
					threadIds.wait();
				} catch (InterruptedException e) {
				}
			}
		}
		
		System.out.println("Finishing");
		closeReaders();
	}
	
	static void putFile(JsonMapper mapper, B3Table b3table, String hashKey, String rangeKey, B3Cell<?>... cells) {

		try {
			BufferedWriter writer = getWriter(b3table);
			synchronized (writer) {
				writer.write(mapper.serialize(new PutBean(hashKey, rangeKey, cells)));
				writer.newLine();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean put(boolean tryToDie, B3Table b3table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		
		//String rangeKey = update.key.getRangeKey();
		Item item ;
		if (rangeKey != null) {
			item = new Item().withPrimaryKey(HASH, hashKey, RANGE, rangeKey);
		} else {
			item = new Item().withPrimaryKey(HASH, hashKey);
		}
		if (cells != null) {
			for (B3Cell<?> c : cells) {
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

		Table dynaTable = B3Bundle.workingBundle.getTable(b3table);
		//System.out.println("DB-PUT " + update);
		if (!readOnly) {
			while (true) {
				try {
					dynaTable.putItem(item);
					break;
				} catch (RuntimeException re) {
					logger.info(Thread.currentThread().getName() + ": " + b3table.name + ": " + re.getMessage());
					if (!tryToDie) {
						return false;
					}
					logger.info(b3table.name + ": Will retry in 100 ms");
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
		}
		return true;
	}
	
	/*public static void putSepc(String hashKey, String rangeKey, String[]... nameValuePairs ) {
		
		Item item = new Item().withPrimaryKey(HASH, hashKey, RANGE, rangeKey);
		if (nameValuePairs != null) {
			for (String[] onePair : nameValuePairs) {
				item = item.withString(onePair[0], onePair[1]);
			}
		}

		//System.out.println("SEPC: " + hashKey + "/" + rangeKey);
		if (!readOnly) {
			B3Bundle.workingBundle.sepcTable.putItem(item);
		}
	}*/

	/*public static void update(B3Update b3update) {
		update(b3update.table, b3update.key.getHashKey(), b3update.key.getRangeKey(), b3update.cells);
	}*/
	
	public static void update(B3Table b3table, String hashKey, String rangeKey, B3Cell<?>... cells) {

		UpdateItemSpec us;
		//String rangeKey = update.key.getRangeKey();
		if (rangeKey != null) {
			us = new UpdateItemSpec().withPrimaryKey(HASH, hashKey, RANGE, rangeKey);
		} else {
			us = new UpdateItemSpec().withPrimaryKey(HASH, hashKey);
		}
		if (cells != null) {
			for (B3Cell<?> c : cells) {
				us = us.addAttributeUpdate(new AttributeUpdate(c.columnName).put(c.value));
			}
		}

		Table dynaTable = B3Bundle.workingBundle.getTable(b3table);
		//System.out.println("DB-UPDATE " + update);
		if (!readOnly) {
			while (true) {
				try {
					dynaTable.updateItem(us);
					break;
				} catch (RuntimeException re) {
					re.printStackTrace();
					logger.info(re.getClass().getName() + ": " + re.getMessage());
					logger.info(b3table.name + ": Will retry in 100 ms");
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
		}
	}
	
	public static void logError(String error) {
		/*Item item = new Item().withPrimaryKey(HASH, BUNDLE_ERROR, RANGE,
				B3Bundle.getWorkingBundleId() + System.currentTimeMillis());
		item = item
				.withString("time", new Date().toString())
				.withString("message", error);
		settingTable.putItem(item);*/
		
		put(true, B3Table.Setting, BUNDLE_ERROR, B3Bundle.getWorkingBundleId() + System.currentTimeMillis(),
				new B3CellString("time", new Date().toString()),
				new B3CellString("message", error));

		logger.error(error);
	}

	public static void updateSetting(B3CellString... cells) {

		update(B3Table.Setting, BUNDLE_HASH, B3Bundle.getWorkingBundleId(), cells);
		/*UpdateItemSpec us = new UpdateItemSpec().withPrimaryKey(
				HASH, BUNDLE_HASH, RANGE, B3Bundle.getWorkingBundleId());
		if (cells != null) {
			for (B3Cell<?> c : cells) {
				us = us.addAttributeUpdate(new AttributeUpdate(c.columnName).put(c.value));
			}
		}
		if (!readOnly) {
			settingTable.updateItem(us);
		}*/
	}

	public static Item get(B3Table b3table, String hashKey, String rangeKey) {
		
		Table table = B3Bundle.workingBundle.getTable(b3table);
		if (rangeKey == null) {
			//System.out.println("DB-GET " + table.getTableName() + ": " + hashKey);
			return table.getItem(HASH, hashKey);
		} else {
			//System.out.println("DB-GET " + table.getTableName() + ": " + hashKey + "@" + rangeKey);
			return table.getItem(HASH, hashKey, RANGE, rangeKey);
		}
	}
	
	public static void delete(B3Table b3table, String hashKey, String rangeKey) {
		
		Table table = B3Bundle.workingBundle.getTable(b3table);
		//System.out.println("DB-DELETE " + b3table.name + " " + hashKey + "@" + rangeKey);
		if (!readOnly) {
			while (true) {
				try {
					if (rangeKey != null) {
						table.deleteItem(HASH, hashKey, RANGE, rangeKey);
					} else {
						table.deleteItem(HASH, hashKey);
					}
					break;
				} catch (RuntimeException re) {
					logger.info(re.getClass().getName() + ": " + re.getMessage());
					logger.info(table.getTableName() + ": Will retry in 100 ms");
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
		}
	}
	
	public static void deleteParallel(B3Table b3table, int segment, int totalSegments) {
		
		HashMap<String, String> nameMap = new HashMap<String, String>();
		nameMap.put("h", "hash");
		
		ScanSpec spec = new ScanSpec()
            //.withMaxResultSize(10)
            .withTotalSegments(totalSegments)
            .withSegment(segment)
            //.withAttributesToGet(RANGE, HASH)
            .withNameMap(nameMap)
            .withFilterExpression(":h = y");
        
		Table table = B3Bundle.workingBundle.getTable(b3table);
        ItemCollection<ScanOutcome> items = table.scan(spec);
        Iterator<Item> iterator = items.iterator();
        Item currentItem = null;
        while (iterator.hasNext()) {
            currentItem = iterator.next();
            String hashKey = currentItem.getString(HASH);
            System.out.print("Scanning " + hashKey);
			/*if (!hashKey.startsWith(bundle.id)) {
				return;
			}*/
			//System.out.println(hashKey);
			//table.deleteItem(HASH, hashKey, RANGE, currentItem.getString(RANGE));
        }
	}
	
	/*private static void deleteBundle(B3Table b3table, final String bundleId) {
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
	}*/
	
	public static ItemCollection<QueryOutcome> query(B3Table b3table, String hashKey) {
		return query(b3table, hashKey, null);
	}
	
	public static ItemCollection<QueryOutcome> query(B3Table b3table, String hashKey, Integer maxResulteSize) {
		
		Table table = B3Bundle.workingBundle.getTable(b3table);
		//System.out.println("QUERY " + table.getTableName() + ": hash=" + hashKey);
		QuerySpec spec = new QuerySpec().withHashKey(HASH, hashKey);
		if (maxResulteSize != null) {
			spec = spec.withMaxResultSize(maxResulteSize);
		}
		return table.query(spec);
	}
	
	public static ItemCollection<QueryOutcome> queryRangeBeginsWith(
			B3Table b3table, String hashKey, String rangeStart) {
		
		Table table = B3Bundle.workingBundle.getTable(b3table);
		QuerySpec spec = new QuerySpec().withHashKey(HASH, hashKey)
				.withRangeKeyCondition(new RangeKeyCondition(RANGE).beginsWith(rangeStart));
		return table.query(spec);
	}
}