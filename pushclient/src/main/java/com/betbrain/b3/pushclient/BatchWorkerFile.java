package com.betbrain.b3.pushclient;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.betbrain.sepc.connector.sportsmodel.EntityChange;

class BatchWorkerFile {
	
	//private static final JsonMapper mapper = new JsonMapper();
	
	static final int BATCHID_DIGIT_COUNT = "00026473973523".length(); //sample batch id: 26473973523
	
	private static BufferedWriter sepcWriter;
	
	//private static Gson gson = new Gson();
	
	static void init() throws IOException {
		sepcWriter = new BufferedWriter(new FileWriter("sepc", false));
	}
	
	/*static void save(EntityChangeBatch batch) throws IOException {

		//new change list to replace EntityChange by its wrapper (we failed serialize EntityUpdate/EntityCreate)
		int changeIndex = 0;
		int batchDigitCount = String.valueOf(batch.getEntityChanges().size()).length();
		for (EntityChange change : batch.getEntityChanges()) {
			ChangeBase wrapper;
			if (change instanceof EntityUpdate) {
				wrapper = new ChangeUpdateWrapper((EntityUpdate) change);
				String error = ((ChangeUpdateWrapper) wrapper).validate();
				if (error != null) {
					DynamoWorker.logError(error);
					continue;
				}
			} else if (change instanceof EntityCreate) {
				wrapper = new ChangeCreateWrapper((EntityCreate) change);
			} else if (change instanceof EntityDelete) {
				wrapper = new ChangeDeleteWrapper((EntityDelete) change);
			} else {
				throw new RuntimeException("Unknown change class: " + change.getClass().getName());
			}
			String hashKey = DynamoWorker.SEPC_CHANGEBATCH + 
					Math.abs(String.valueOf(batch.getId()).hashCode() % B3Table.DIST_FACTOR);
			String rangeKey = B3Key.zeroPadding(BATCHID_DIGIT_COUNT, batch.getId()) +
					B3Table.KEY_SEP + B3Key.zeroPadding(batchDigitCount, changeIndex);
			changeIndex++;
			JsonObject json = new JsonObject();
			json.addProperty("HASH", hashKey);
			json.addProperty("RANGE", rangeKey);
			json.addProperty(DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime()));
			json.addProperty(DynamoWorker.SEPC_CELLNAME_JSON, mapper.serialize(wrapper));
			sepcWriter.write(gson.toJson(json));
			sepcWriter.newLine();
		}
	}*/
	
	static void save(EntityChange change) throws IOException {

		sepcWriter.write(change.toString());
		sepcWriter.newLine();
	}
}
