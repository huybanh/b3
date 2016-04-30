package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.b3.pushclient.EntityChangeBase;
import com.betbrain.b3.pushclient.EntityCreateWrapper;
import com.betbrain.b3.pushclient.EntityDeleteWrapper;
import com.betbrain.b3.pushclient.EntityUpdateWrapper;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.B3Update;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public abstract class B3Entity<E extends Entity/*, K extends B3Key*/> {

	public E entity;
	
	protected B3Entity() {
	}
	
	/*protected B3Entity(E entity) {
		this.entity = entity;
	}*/
	
	//abstract public K getB3KeyMain();
	
	private LinkedList<EntityLink> downlinks;
	
	abstract protected void getDownlinkedEntitiesInternal();
	
	protected final void addDownlink(String name, B3Entity<?> linkedEntity) {
		if (linkedEntity == null) {
			return;
		}
		downlinks.add(new EntityLink(name, linkedEntity));
	}
	
	protected final void addDownlink(String name, Class<?> linkedEntityClazz, Long linkedEntityId) {
		if (linkedEntityId == null) {
			return;
		}
		downlinks.add(new EntityLink(name, linkedEntityClazz, linkedEntityId));
	}
	
	public final EntityLink[] getDownlinkedEntities() {
		downlinks = new LinkedList<EntityLink>();
		getDownlinkedEntitiesInternal();
		EntityLink[] links = downlinks.toArray(new EntityLink[downlinks.size()]);
		downlinks = null;
		return links;
	}
	
	abstract public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper);
	
	/*@SuppressWarnings("rawtypes")
	static <E extends B3Entity, F> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap, B3Bundle JsonMapper mapper) {
		
		return build(id, e, clazz, masterMap, mapper, true);
	}*/
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static <E extends B3Entity> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap, 
			JsonMapper mapper/*, boolean depthBuilding*/) {
		
		if (id == null) {
			return null;
		}
		
		Entity one;
		if (masterMap != null) {
			one = lookup(id, clazz, masterMap);
		} else {
			one = lookupB3(id, clazz, mapper);
		}
		if (one == null) {
			return null;
		}
		e.entity = one;
		//if (depthBuilding) {
			e.buildDownlinks(masterMap, mapper);
		//}
		return e;
	}
	
	private static Entity lookup(Long id, Class<? extends Entity> clazz, HashMap<String, HashMap<Long, Entity>> masterMap) {
		HashMap<Long, Entity> subMap = masterMap.get(clazz.getName());
		if (subMap == null) {
			InitialDumpDeployer.linkingErrors.add("Found zero entities of " + clazz.getName());
			return null;
		}
		Entity one = subMap.get(id);
		if (one == null) {
			InitialDumpDeployer.linkingErrors.add("Missed ID " + id + " of " + clazz.getName());
			return null;
		}
		return one;
	}
	
	private static Entity lookupB3(long id, Class<? extends Entity> clazz, JsonMapper mapper) {
		
		B3KeyEntity entityKey = new B3KeyEntity(clazz, id);
		Entity foundEntity = entityKey.load(mapper);
		if (foundEntity == null) {
			System.out.println("Ignoring entity due to missing linked entity: " + clazz.getName() + "@" + id);
			return null;
		}
		return foundEntity;
	}
	
	@SuppressWarnings("unchecked")
	static <E extends Entity> B3Entity<E> deserialize(JsonMapper mapper, Item item, B3Entity<E> b3entity, String propertyName) {
		String json = item.getString(propertyName);
		if (json != null) {
			b3entity.entity = (E) mapper.deserialize(json);
		}
		return b3entity;
	}
	
	public static void applyChange(EntityChangeBase change, JsonMapper mapper) {
		
		Class<? extends B3Entity<?>> b3class = ModelShortName.getB3Class(change.getEntityClassName());
		if (b3class == null) {
			System.out.println("Ignoring unconfigured change handler " + change.getEntityClassName());
			return;
		}
		try {
			B3Entity<?> b3entity = b3class.newInstance();
			//b3entity.entity = change.getEntity();
			if (change instanceof EntityCreateWrapper) {
				b3entity.applyChangeCreate((EntityCreateWrapper) change, mapper);
				
			} else if (change instanceof EntityUpdateWrapper) {
				b3entity.applyChangeUpdate((EntityUpdateWrapper) change, mapper);
				
			} else if (change instanceof EntityDeleteWrapper) {
				b3entity.applyChangeDelete((EntityDeleteWrapper) change, mapper);
			} else {
				throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
			}
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	void applyChangeCreate(EntityCreateWrapper create, JsonMapper mapper) {
		
		//table entity
		this.entity = (E) create.getEntity();
		B3KeyEntity entityKey = new B3KeyEntity(this.entity);
		String newEntityJson = mapper.serialize(this.entity);
		B3CellString cells = new B3CellString(B3Table.CELL_LOCATOR_THIZ, newEntityJson);
		B3Update b3update = new B3Update(B3Table.Entity, entityKey, cells);
		DynamoWorker.put(b3update);
		
	}

	void applyChangeUpdate(EntityUpdateWrapper update, JsonMapper mapper) {
		
	}

	void applyChangeDelete(EntityDeleteWrapper delete, JsonMapper mapper) {
		
	}

}
