package com.betbrain.b3.data;

import java.util.HashMap;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class ChangeBatchDeployer {
	
	private B3Bundle bundle;
	
	private JsonMapper mapper = new JsonMapper();
	
	public ChangeBatchDeployer(B3Bundle bundle) {
		this.bundle = bundle;
	}

	public void deployChangeBatches() {

		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			
			final int distFinal = dist;
			ItemCollection<QueryOutcome> coll = DynamoWorker.query(
				bundle, B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + distFinal);

			IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
			int changeBatchCount = 0;
			while (iter.hasNext()) {
				Item item = iter.next();
				//String batchId = item.getString(DynamoWorker.RANGE);
				//String createTime = item.getString(DynamoWorker.SEPC_CELLNAME_CREATETIME);
				String changesJson = item.getString(DynamoWorker.SEPC_CELLNAME_CHANGES);
				@SuppressWarnings("unchecked")
				List<Object> changes = (List<Object>) mapper.deserialize(changesJson);
				//System.out.println(changes);
				changeBatchCount++;
				if (changeBatchCount % 100 == 0) {
					System.out.println("Change-batch count: " + changeBatchCount);
				}
			}
		}
	}
}
