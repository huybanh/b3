package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.EntityChangeBase;
import com.betbrain.b3.pushclient.JsonMapper;

public class ChangeBatchDeployer {
	
	private JsonMapper mapper = new JsonMapper();
	
	private ExecutorService executor;
	
	public static void main(String[] args) {

		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DEPLOYWAIT);
		DynamoWorker.setWorkingBundleStatus(DynamoWorker.BUNDLE_STATUS_PUSHING);
		new ChangeBatchDeployer().deployChangeBatches();
	}
	
	public ChangeBatchDeployer() {
		executor = Executors.newFixedThreadPool(5);
	}

	public void deployChangeBatches() {

		while (true) {
			ArrayList<Object> allChanges = queryForChanges();
			System.out.println("Total changes to deploy: " + allChanges.size());
			for (Object one : allChanges) {
				B3Entity.applyChange((EntityChangeBase) one, mapper);
			}
			//break; //for testing only
		}
	}
	
	private ArrayList<Object> queryForChanges() {
		
		LinkedList<Future<Integer>> executions = new LinkedList<Future<Integer>>(); 
		final ArrayList<Object> allChanges = new ArrayList<Object>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			
			final int distFinal = dist;
			Future<Integer> oneExecution = executor.submit(new Runnable() {
				
					public void run() {
						ItemCollection<QueryOutcome> coll = DynamoWorker.query(
								B3Table.SEPC, DynamoWorker.SEPC_CHANGEBATCH + distFinal);
	
						IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
						int changeBatchCount = 0;
						while (iter.hasNext()) {
							Item item = iter.next();
							//String batchId = item.getString(DynamoWorker.RANGE);
							//String createTime = item.getString(DynamoWorker.SEPC_CELLNAME_CREATETIME);
							String changesJson = item.getString(DynamoWorker.SEPC_CELLNAME_CHANGES);
							//System.out.println(changesJson);
							@SuppressWarnings("unchecked")
							List<Object> changes = (List<Object>) mapper.deserialize(changesJson);
							for (Object obj : changes) {
								if (obj != null) {
									allChanges.add(obj);
								} else {
									System.out.println("NULL CHANGE: " + changesJson);
									Thread.dumpStack();
								}
							}
							//System.out.println(changes);
							changeBatchCount++;
							if (changeBatchCount % 100 == 0) {
								System.out.println("Change-batch count: " + changeBatchCount);
							}
						}
					}
				}, 1);
			executions.add(oneExecution);
		}
		
		for (Future<Integer> one : executions) {
			while (true) {
				try {
					one.get();
				} catch (InterruptedException e) {
					continue;
				} catch (ExecutionException e) {
					continue;
				}
				break;
			}
		}
		return allChanges;
	}
}
