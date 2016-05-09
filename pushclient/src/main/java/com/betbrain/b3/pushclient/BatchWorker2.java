package com.betbrain.b3.pushclient;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.ChangeBase;
import com.betbrain.b3.data.ChangeCreateWrapper;
import com.betbrain.b3.data.ChangeDeleteWrapper;
import com.betbrain.b3.data.ChangeUpdateWrapper;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityChangeBatch;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;

class BatchWorker2 implements Runnable {
	
    private final Logger logger = Logger.getLogger(this.getClass());
	
	final ArrayList<EntityChangeBatch> batches;
	
	private final JsonMapper mapper = new JsonMapper();
	
	private static long printTimestamp;
	
	//private static boolean firstBatch = true;
	
	static final int BATCHID_DIGIT_COUNT = "00026473973523".length(); //sample batch id: 26473973523
	
	BatchWorker2(ArrayList<EntityChangeBatch> batches) {
		this.batches = batches;
	}

	@Override
	public void run() {

		logger.info("Started a batch-deploying thread...");
		//int printCount = 0;
		while (true) {
			EntityChangeBatch batch;
			synchronized (batches) {
				if (batches.isEmpty()) {
					try {
						batches.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				batch = batches.remove(0);
				if (System.currentTimeMillis() - printTimestamp > 5000) {
					printTimestamp = System.currentTimeMillis();
					logger.info(Thread.currentThread().getName() + ": Batches in queue: " + batches.size());
				}
			}

			//new change list to replace EntityChange by its wrapper (we failed serialize EntityUpdate/EntityCreate)
			//LinkedList<Object> changeList = new LinkedList<Object>();
			int i = 0;
			int batchDigitCount = String.valueOf(batch.getEntityChanges().size()).length();
			for (EntityChange change : batch.getEntityChanges()) {
				//nameValuePairs.add(new String[] {String.valueOf(i++), serializeChange(change)});
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
				String hashKey = generateChangeBatchHashKey(batch.getId());
				String rangeKey = B3Key.zeroPadding(BATCHID_DIGIT_COUNT, batch.getId()) +
						B3Table.KEY_SEP + B3Key.zeroPadding(batchDigitCount, i);
				i++;
				DynamoWorker.put(true, B3Table.SEPC, hashKey, rangeKey,
					new B3CellString(DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())),
					new B3CellString(DynamoWorker.SEPC_CELLNAME_JSON, mapper.serialize(wrapper)));
			}
			
			//put
			/*String rangeKey = String.valueOf(batch.getId());
			String hashKey = generateHashKey(batch.getId());
			DynamoWorker.putSepc(hashKey, rangeKey,
				new String[] {DynamoWorker.SEPC_CELLNAME_CREATETIME, mapper.serialize(batch.getCreateTime())},
				new String[] {DynamoWorker.SEPC_CELLNAME_CHANGES, mapper.serialize(changeList)});*/
			
		}
	}
	
	static String generateChangeBatchHashKey(long batchId) {
		return DynamoWorker.SEPC_CHANGEBATCH + Math.abs(String.valueOf(batchId).hashCode() % B3Table.DIST_FACTOR);
	}
}
