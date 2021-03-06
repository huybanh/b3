package com.betbrain.b3.data;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.model.B3Entity;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3Key {
	
	//static final SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyyyHHmmss");
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	
	static final ExecutorService exectuorService = Executors.newFixedThreadPool(20);
	
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

	/*@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList<?> listEntities(final boolean revisions, final Class<? extends B3Entity<?>> b3class,
			final JsonMapper jsonMapper, final String... colNames ) {
		
		final String hashKeySuffix;
		if (revisions) {
			hashKeySuffix = B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKeySuffix = "";
		}

		String hashKeyPart = getHashKeyInternal();
		if (hashKeyPart != null) {
			ArrayList[] outLists = new ArrayList[] {new ArrayList()};
			query(hashKeyPart + hashKeySuffix, revisions, b3class, outLists, 0, jsonMapper, colNames);
			return outLists[0];
		}

		final String hashKeyPrefix;
		String prefix = getHashKeyPrefix();
		if (prefix == null) {
			hashKeyPrefix = "";
		} else {
			hashKeyPrefix = prefix;
		}
		
		final ArrayList[] outLists = new ArrayList[B3Table.DIST_FACTOR];
		for (int i = 0; i < outLists.length; i++) {
			outLists[i] = new ArrayList();
		}
		
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
							if (thisIndex == outLists.length) {
								index.notifyAll();
								threadIds.remove(oneThreadId);
								return;
							}
							index[0] = thisIndex + 1;
						}
						query(hashKeyPrefix + thisIndex + hashKeySuffix, 
								revisions, b3class, outLists, thisIndex, jsonMapper, colNames);
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
		
		ArrayList resultList = new ArrayList<>();
		for (int i = 0; i < outLists.length; i++) {
			resultList.addAll(outLists[i]);
		}
		return resultList;
	}*/
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList<?> listEntities(final boolean revisions, final Class<? extends B3Entity<?>> b3class,
			final JsonMapper jsonMapper, final String... colNames ) {
		
		final String hashKeySuffix;
		if (revisions) {
			hashKeySuffix = B3Table.KEY_SUFFIX_REVISION;
		} else {
			hashKeySuffix = "";
		}

		String hashKeyPart = getHashKeyInternal();
		if (hashKeyPart != null) {
			ArrayList[] outLists = new ArrayList[] {new ArrayList()};
			query(hashKeyPart + hashKeySuffix, revisions, b3class, outLists, 0, jsonMapper, colNames);
			return outLists[0];
		}

		final String hashKeyPrefix;
		String prefix = getHashKeyPrefix();
		if (prefix == null) {
			hashKeyPrefix = "";
		} else {
			hashKeyPrefix = prefix;
		}
		
		final ArrayList[] outLists = new ArrayList[B3Table.DIST_FACTOR];
		for (int i = 0; i < outLists.length; i++) {
			outLists[i] = new ArrayList();
		}
		
		LinkedList<Future<?>> futures = new LinkedList<>();
		for (int i = 0; i < B3Table.DIST_FACTOR; i++) {
			final int oneDist = i;
			Runnable task = new Runnable() {
				public void run() {
					query(hashKeyPrefix + oneDist + hashKeySuffix, 
							revisions, b3class, outLists, oneDist, new JsonMapper(), colNames);
				}
			};
			futures.add(exectuorService.submit(task));
		}
		
		waitForTaskCompletions(futures);
		ArrayList resultList = new ArrayList<>();
		for (int i = 0; i < outLists.length; i++) {
			resultList.addAll(outLists[i]);
		}
		return resultList;
	}
	
	@SuppressWarnings("unchecked")
	private void query(String hashKey, boolean revisions, Class<? extends B3Entity<?>> b3class, 
			@SuppressWarnings("rawtypes") /*ArrayList list,*/ 
			ArrayList[] outLists, Integer arrayIndex, JsonMapper jsonMapper,
			String... colNames) {
		
		long time = System.currentTimeMillis();
		B3ItemIterator it = DynamoWorker.query(getTable(), hashKey, getRangeKey(), rangeKeyEnd, colNames);
		while (it.hasNext()) {
			Item item = it.next();
			B3Entity<Entity> b3entity;
			try {
				b3entity = (B3Entity<Entity>) b3class.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			
			if (!b3entity.load(item, null, jsonMapper)) {
				String rangeKey = item.getString(DynamoWorker.RANGE);
				outLists[arrayIndex].add(rangeKey);
				System.out.println("Got range: " + rangeKey);
				continue;
			}
			Object obj;
			if (revisions) {
				String rangeKey = item.getString(DynamoWorker.RANGE);
				long revisionTime = Long.parseLong(rangeKey.substring(rangeKey.lastIndexOf('/') + 1));
				obj = new RevisionedEntity<>(revisionTime, b3entity);
			} else {
				//String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
				//Entity entity = (Entity) jsonMapper.deserialize(json);
				//b3entity.entity = entity;
				obj = b3entity;
			}
			outLists[arrayIndex].add(obj);
		}
		System.out.println("Queried " + getTable().name + ": " + hashKey + "@" + getRangeKey() +
				", returned " + outLists[arrayIndex].size() + " in " + (System.currentTimeMillis() - time) + " ms");
	}
	
	static void waitForTaskCompletions(LinkedList<Future<?>> futures) {
		for (Future<?> f : futures) {
			while (true) {
				try {
					f.get();
				} catch (InterruptedException e) {
					continue;
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
				break;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	static <E> void waitForTaskCompletions(LinkedList<Future<E>> futures, ArrayList<E> taskResultList) {
		for (Future<E> f : futures) {
			while (true) {
				try {
					E e = f.get();
					if (e instanceof Collection) {
						taskResultList.addAll((Collection<E>) e);
					} else {
						taskResultList.add(e);
					}
				} catch (InterruptedException e) {
					continue;
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
				break;
			}
		}
	}
}
