package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.InitialPutHandler;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
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
	
	abstract public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap);
	
	@SuppressWarnings("rawtypes")
	static <E extends B3Entity, F> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap) {
		
		return build(id, e, clazz,masterMap, true);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static <E extends B3Entity> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap, boolean depthBuilding) {
		
		if (id == null) {
			return null;
		}
		HashMap<Long, Entity> subMap = masterMap.get(clazz.getName());
		if (subMap == null) {
			InitialPutHandler.linkingErrors.add("Found zero entities of " + clazz.getName());
			return null;
		}
		Entity one = subMap.get(id);
		if (one == null) {
			InitialPutHandler.linkingErrors.add("Missed ID " + id + " of " + clazz.getName());
			return null;
		}
		e.entity = one;
		if (depthBuilding) {
			e.buildDownlinks(masterMap);
		}
		return e;
	}
	
	@SuppressWarnings("unchecked")
	static <E extends Entity> B3Entity<E> deserialize(JsonMapper mapper, Item item, B3Entity<E> b3entity, String propertyName) {
		String json = item.getString(propertyName);
		if (json != null) {
			b3entity.entity = (E) mapper.deserialize(json);
		}
		return b3entity;
	}

}
