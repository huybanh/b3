package com.betbrain.b3.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;

/**
 * Key spec: sportId/eventTypeId/[EVENT|EVENTPART]/eventId
 *
 */
public class B3KeyEntity extends B3Key {

	final String classShortName;
	
	final Long id;

	public B3KeyEntity(Entity entity) {
		super();
		classShortName = EntitySpec2.getShortName(entity.getClass().getName()); 
		id = entity.getId();
	}

	public B3KeyEntity(Class<? extends Entity> clazz, long id) {
		super();
		classShortName = EntitySpec2.getShortName(clazz.getName()); 
		this.id = id;
	}

	public B3KeyEntity(String className, long id) {
		super();
		classShortName = EntitySpec2.getShortName(className); 
		this.id = id;
	}

	public B3KeyEntity(Class<?> clazz) {
		super();
		classShortName = EntitySpec2.getShortName(clazz.getName()); 
		id = null;
	}
	
	@Override
	B3Table getTable() {
		return B3Table.Entity;
	}
	
	@Override
	boolean isDetermined() {
		return classShortName != null && id != null;
	}
	
	@Override
	public String getHashKeyInternal() {
		if (id == null) {
			//return classShortName;
			return null;
		}
		if (version2) {
			return classShortName + Math.abs(id % B3Table.DIST_FACTOR);
		}
		return classShortName + Math.abs(((Long) id).hashCode() % B3Table.DIST_FACTOR);
	}
	
	@Override
	String getHashKeyPrefix() {
		return classShortName;
	}
	
	@Override
	String getRangeKeyInternal() {
		if (id == null) {
			return null;
		}
		return String.valueOf(id); 
	}
	
	/*@SuppressWarnings("unchecked")
	public <E extends Entity> ArrayList<E> listEntities(JsonMapper jsonMapper) {
		ArrayList<E> list = new ArrayList<E>();
		//int i = hardLimit;
		for (int distFactor = 0; distFactor < B3Table.DIST_FACTOR; distFactor++) {
			B3ItemIterator it = DynamoWorker.query(B3Table.Entity, classShortName + distFactor);
			while (it.hasNext()) {
				Item item = it.next();
				String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
				Entity entity = jsonMapper.deserializeEntity(json);
				System.out.println(entity);
				list.add((E) entity);
			}
		}
		return list;
	}*/

	public static <E extends Entity> ArrayList<E> load(JsonMapper mapper, Class<E> clazz, long id) {
		ArrayList<Long> idList = new ArrayList<Long>();
		idList.add(id);
		return load(mapper, clazz, idList);
	}

	public static <E extends Entity> ArrayList<E> load(final JsonMapper mapper,
			final Class<E> clazz, ArrayList<Long> idList) {
		
		LinkedList<Future<E>> futures = new LinkedList<>();
		for (Long id : idList) {
			final long entityId = id;
			Callable<E> task = new Callable<E>() {
				
				@Override
				public E call() {
					return new B3KeyEntity(clazz, entityId).load(new JsonMapper());
				}
			};
			futures.add(exectuorService.submit(task));
		}
		
		ArrayList<E> list = new ArrayList<E>();
		waitForTaskCompletions(futures, list);
		return list;
	}

	public <E extends Entity> E load(JsonMapper mapper) {
		//System.out.println("Loading entity " + getHashKey() + "@" + getRangeKey());
		Item item = DynamoWorker.get(B3Table.Entity, getHashKey(), getRangeKey());
		if (item == null) {
			return null;
		}
		String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
		@SuppressWarnings("unchecked")
		E entity = (E) mapper.deserialize(json);
		//System.out.println(entity);
		return entity;
	}
}
