package com.betbrain.b3.data;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3Key {
	
	//static final SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	
	//TODO remove this flag
	public static boolean version2 = true;
	
	private String revisionId;
	
	public String rangeKeyEnd;
	
	abstract B3Table getTable();
	
	abstract boolean isDetermined();
	
	abstract String getRangeKeyInternal();
	
	abstract String getHashKeyInternal();
	
	String getHashKeyPrefix() {
		return null;
	}
	
	public final String getRangeKey() {
		if (revisionId != null) {
			return getRangeKeyInternal() + B3Table.KEY_SEP + revisionId;
		} else {
			return getRangeKeyInternal();
		}
	}
	
	public final String getHashKey() {
		if (revisionId != null) {
			return getHashKeyInternal() + B3Table.KEY_SUFFIX_REVISION;
		} else {
			return getHashKeyInternal();
		}
	}
	
	/*protected int module(int l, int m) {
		return Math.abs(l % m);
	}*/
	
	public void setRevisionId(String revisionId) {
		this.revisionId = revisionId;
	}
	
	private static final String ZEROS = "0000000000000000000000000000000000000000000000000000000000";
	
	public static String zeroPadding(int length, long number) {
		String s = String.valueOf(number);
		if (s.length() > length) {
			throw new RuntimeException("Number has more than " + length + " digits: " + number);
		}
		return ZEROS.substring(0, length - s.length()) + s;
	}
	
	public static void main(String[] args) {
		System.out.println(zeroPadding(1, 1));
		System.out.println(zeroPadding(5, 1));
		System.out.println(zeroPadding(5, 23));
	}

	@SuppressWarnings("unchecked")
	public ArrayList<?> listEntities(final boolean revisions, 
			final Class<? extends B3Entity<?>> b3class, final JsonMapper jsonMapper ) {
		
		final String hashKeySuffix;
		if (revisions) {
			hashKeySuffix = B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKeySuffix = "";
		}

		@SuppressWarnings("rawtypes")
		ArrayList list = new ArrayList();
		String hashKeyPart = getHashKeyInternal();
		if (hashKeyPart != null) {
			query(hashKeyPart + hashKeySuffix, revisions, b3class, list, null, null, jsonMapper);
		} else {

			final String hashKeyPrefix;
			String prefix = getHashKeyPrefix();
			if (prefix == null) {
				hashKeyPrefix = "";
			} else {
				hashKeyPrefix = prefix;
			}
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
							query(hashKeyPrefix + thisIndex + hashKeySuffix, 
									revisions, b3class, null, obj, thisIndex, jsonMapper);
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
	private void query(String hashKey, boolean revisions, Class<? extends B3Entity<?>> b3class, 
			@SuppressWarnings("rawtypes") ArrayList list, 
			Object[] array, Integer arrayIndex, JsonMapper jsonMapper) {
		
		/*Integer partitionRecordsLimit;
		if (list == null) {
			partitionRecordsLimit = 1;
		} else {
			partitionRecordsLimit = null;
		}*/
		//Class<? extends B3Entity<?>> b3class = getEntitySpec().b3class;
		//System.out.println("Querying " + getTable().name + ": " + hashKey + "@" + getRangeKey());
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), rangeKeyEnd, null/*partitionRecordsLimit*/);
		
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
				Entity entity = (Entity) jsonMapper.deserialize(json);
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
