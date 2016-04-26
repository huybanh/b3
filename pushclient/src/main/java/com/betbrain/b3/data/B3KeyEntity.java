package com.betbrain.b3.data;

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
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
		classShortName = ModelShortName.get(entity.getClass().getName()); 
		id = entity.getId();
	}

	public B3KeyEntity(Class<? extends Entity> clazz, long id) {
		super();
		classShortName = ModelShortName.get(clazz.getName()); 
		this.id = id;
	}

	public B3KeyEntity(Class<?> clazz) {
		super();
		classShortName = ModelShortName.get(clazz.getName()); 
		id = null;
	}
	
	@Override
	boolean isDetermined() {
		return id != null;
	}
	
	protected String getHashKey() {
		if (id == null) {
			return classShortName;
		}
		return classShortName + Math.abs(((Long) id).hashCode() % B3Table.DIST_FACTOR);
	}
	
	@Override
	String getRangeKey() {
		return String.valueOf(id); 
	}
	
	static final int hardLimit = 50;
	
	@SuppressWarnings("unchecked")
	public <E extends Entity> ArrayList<E> listEntities() {
		ArrayList<E> list = new ArrayList<E>();
		int i = hardLimit;
		for (int distFactor = 0; distFactor < B3Table.DIST_FACTOR; distFactor++) {
			ItemCollection<QueryOutcome> coll = DynamoWorker.query(B3Table.Entity, classShortName + distFactor);
			IteratorSupport<Item, QueryOutcome> it = coll.iterator();
			while (it.hasNext()) {
				if (--i <= 0) {
					break;
				}
				Item item = it.next();
				String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
				Entity entity = JsonMapper.DeserializeF(json);
				System.out.println(entity);
				list.add((E) entity);
			}
		}
		return list;
	}

	public static <E extends Entity> ArrayList<E> load(Class<E> clazz, long id) {
		ArrayList<Long> idList = new ArrayList<Long>();
		idList.add(id);
		return load(clazz, idList);
	}

	public static <E extends Entity> ArrayList<E> load(Class<E> clazz, ArrayList<Long> idList) {
		ArrayList<E> list = new ArrayList<E>();
		for (Long id : idList) {
			B3KeyEntity key = new B3KeyEntity(clazz, id);
			Item item = DynamoWorker.get(B3Table.Entity, key.getHashKey(), key.getRangeKey());
			if (item == null) {
				continue;
			}
			String json = item.getString(B3Table.CELL_LOCATOR_THIZ);
			@SuppressWarnings("unchecked")
			E entity = (E) JsonMapper.DeserializeF(json);
			System.out.println(entity);
			list.add(entity);
		}
		return list;
	}
}
