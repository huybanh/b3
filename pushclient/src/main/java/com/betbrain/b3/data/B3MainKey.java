package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.LinkedList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3MainKey<E extends Entity> extends B3Key {
	
	abstract EntitySpec2 getEntitySpec();

	@SuppressWarnings("unchecked")
	public ArrayList<?> listEntities(final boolean revisions, final JsonMapper jsonMapper ) {
		
		final String hashKeySuffix;
		if (revisions) {
			hashKeySuffix = B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKeySuffix = "";
		}

		@SuppressWarnings("rawtypes")
		ArrayList list = new ArrayList();
		String hashKey = getHashKeyInternal();
		if (hashKey != null) {
			query(hashKey + hashKeySuffix, revisions, list, null, null, jsonMapper);
		} else {

			final Object[] obj = new Object[B3Table.DIST_FACTOR];
			final int[] index = new int[] {0};
			final LinkedList<Object> threadIds = new LinkedList<>();
			for (int i = 0; i < 20; i++) {
				final Object oneThreadId = new Object();
				threadIds.add(oneThreadId);
				new Thread() {
					public void run() {
						while (true) {
							int thisIndex;
							synchronized (index) {
								thisIndex = index[0];
								if (thisIndex == obj.length) {
									index.notifyAll();
									threadIds.remove(oneThreadId);
									return;
								}
								index[0] = thisIndex + 1;
							}
							query(thisIndex + hashKeySuffix, revisions, null, obj, thisIndex, jsonMapper);
						}
					}
				}.start();
			}
			
			synchronized (index) {
				while (true) {
					if (!threadIds.isEmpty()) {
						try {
							index.wait();
						} catch (InterruptedException e) {
						}
						continue;
					}
					break;
				}
			}
			for (int i = 0; i < B3Table.DIST_FACTOR; i++) {
				if (obj[i] != null) {
					list.add(obj[i]);
				}
			}
		}
		
		return list;
	}
	
	@SuppressWarnings("unchecked")
	private void query(String hashKey, boolean revisions, 
			@SuppressWarnings("rawtypes") ArrayList list, 
			Object[] array, Integer arrayIndex, JsonMapper jsonMapper) {
		
		Integer partitionRecordsLimit = null;
		if (list == null) {
			partitionRecordsLimit = 1;
		}
		Class<? extends B3Entity<?>> b3class = getEntitySpec().b3class;
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), partitionRecordsLimit);
		
		while (it.hasNext()) {
			Item item = it.next();
			B3Entity<Entity> b3entity;
			try {
				b3entity = (B3Entity<Entity>) b3class.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			//b3entity.entity = entity;
			b3entity.load(item, jsonMapper);
			Object obj;
			if (revisions) {
				String rangeKey = item.getString(DynamoWorker.RANGE);
				long revisionTime = Long.parseLong(rangeKey.substring(rangeKey.lastIndexOf('/') + 1));
				obj = new RevisionedEntity<>(revisionTime, b3entity);
			} else {
				String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
				E entity = (E) jsonMapper.deserialize(json);
				b3entity.entity = entity;
				obj = b3entity;
			}
			
			//System.out.println("Got " + obj);
			if (list != null) {
				list.add(obj);
			} else {
				array[arrayIndex] = obj;
				break;
			}
		}
	}

}
