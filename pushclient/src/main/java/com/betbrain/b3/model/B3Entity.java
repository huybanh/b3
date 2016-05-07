package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.pushclient.EntityChangeBase;
import com.betbrain.b3.pushclient.EntityCreateWrapper;
import com.betbrain.b3.pushclient.EntityDeleteWrapper;
import com.betbrain.b3.pushclient.EntityUpdateWrapper;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.betbrain.b3.data.B3Cell;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3KeyLookup;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Source;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public abstract class B3Entity<E extends Entity/*, K extends B3Key*/> {
	
    //private static final Logger logger = Logger.getLogger(B3Entity.class.getName());

	public E entity;
	
	private EntitySpec2 entitySpec;
	
	protected B3Entity() {
	}
	
	/*protected B3Entity(E entity) {
		this.entity = entity;
	}*/
	
	//abstract public K getB3KeyMain();
	
	private LinkedList<EntityLink> downlinks;
	
	private boolean workingOnLinkNamesOnly = false;
	
	abstract protected void getDownlinkedEntitiesInternal();
	
	protected final void addDownlink(String name, B3Entity<?> linkedEntity) {
		
		if (workingOnLinkNamesOnly) {
			downlinks.add(new EntityLink(name, null));
			return;
		}
		
		if (linkedEntity == null) {
			return;
		}
		downlinks.add(new EntityLink(name, linkedEntity));
	}
	
	protected final void addDownlinkUnfollowed(String name, Class<?> linkedEntityClazz/*, Long linkedEntityId*/) {
		
		if (workingOnLinkNamesOnly) {
			downlinks.add(new EntityLink(name, null));
			return;
		}
		
		Long linkedEntityId = (Long) BeanUtil.getPropertyValue(entity, name);
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
	
	public final EntityLink[] getDownlinkedNames() {
		workingOnLinkNamesOnly = true;
		downlinks = new LinkedList<EntityLink>();
		getDownlinkedEntitiesInternal();
		EntityLink[] links = downlinks.toArray(new EntityLink[downlinks.size()]);
		downlinks = null;
		return links;
	}
	
	abstract public void buildDownlinks(boolean forMainKeyOnly, HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper);
	
	/*@SuppressWarnings("rawtypes")
	static <E extends B3Entity, F> E build(Long id, E e, Class<? extends Entity> clazz,
			HashMap<String, HashMap<Long, Entity>> masterMap, B3Bundle JsonMapper mapper) {
		
		return build(id, e, clazz, masterMap, mapper, true);
	}*/
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static <E extends B3Entity> E build(boolean forMainKeyOnly, Long id, E e, Class<? extends Entity> clazz,
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
			e.buildDownlinks(forMainKeyOnly, masterMap, mapper);
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
	
	B3Key createMainKey() {
		return null;
	}
	
	String getRevisionId() {
		throw new RuntimeException();
	}
	
	public static void applyChange(String createTime, EntityChangeBase change, JsonMapper mapper) {
		
		/*if (true) {
			System.out.println(change);
			return;
		}*/
		if (Source.class.getName().equals(change.getEntityClassName())) {
			if (change instanceof EntityUpdateWrapper) {
				List<String> changedNames = ((EntityUpdateWrapper) change).getB3PropertyNames();
				if (changedNames == null || 
						(changedNames.size() == 1 && changedNames.contains(Source.PROPERTY_NAME_lastCollectedTime))) {
					
					//skip this trivial change
					return;
				}
			}
		}
		
		EntitySpec2 entitySpec = EntitySpec2.get(change.getEntityClassName());
		if (entitySpec == null /*|| entitySpec.b3class == null*/) {
			System.out.println("Ignoring unconfigured change handler " + change);
			return;
		}
		
		B3Entity<?> b3entity;
		try {
			b3entity = entitySpec.b3class.newInstance();
			b3entity.entitySpec = entitySpec;
			b3entity.applyChangeInternal(createTime, change, mapper);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void applyChangeInternal(String createTime, EntityChangeBase change, JsonMapper mapper) {
		
		if (change instanceof EntityCreateWrapper) {
			System.out.println("CHANGE-CREATE: " + change);
			this.entity = (E) ((EntityCreateWrapper) change).getEntity();
			boolean forMainKeyOnlyFalse = false;
			buildDownlinks(forMainKeyOnlyFalse, null, mapper);
			putCurrent(mapper);
			if (entitySpec.revisioned) {
				putRevision(createTime, mapper);
			}
		
		} else if (change instanceof EntityDeleteWrapper) {
			System.out.println("CHANGE-DELETE: " + change);
			EntityDeleteWrapper delete = (EntityDeleteWrapper) change;
			B3KeyEntity entityKey = new B3KeyEntity(delete.getEntityClassName(), delete.getEntityId());
			this.entity = (E) entityKey.load(mapper);
			if (this.entity == null) {
				DynamoWorker.logError("Got change-delete, but no entity found: " + 
						delete.getEntityClassName() + "/" + delete.getEntityId());
				return;
			}
			boolean forMainKeyOnlyFalse = true;
			buildDownlinks(forMainKeyOnlyFalse, null, mapper);
			deleteCurrent(mapper);
			
		} else if (change instanceof EntityUpdateWrapper) {
			
			System.out.println("CHANGE-UPDATE: " + change);
			EntityUpdateWrapper update = (EntityUpdateWrapper) change;
			B3KeyEntity entityKey = new B3KeyEntity(update.getEntityClassName(), update.getEntityId());
			this.entity = entityKey.load(mapper);
			if (this.entity == null) {
				DynamoWorker.logError("Got change-update, but no entity found: " + 
						update.getEntityClassName() + "/" + update.getEntityId());
				return;
			}
			//System.out.println("BEFORE: " + ((BettingOffer) targetEntity).getLastChangedTime().getTime());
			update.applyChanges(this.entity);
			//System.out.println("AFTER: " + ((BettingOffer) targetEntity).getLastChangedTime().getTime());
			//this.entity = (E) targetEntity;
			
			if (entitySpec.revisioned) {
				boolean forMainKeyOnlyFalse = false;
				buildDownlinks(forMainKeyOnlyFalse, null, mapper);
				putRevision(createTime, mapper);
				if (entitySpec.isStructuralChange(update)) {
					deleteCurrent(mapper);
					putCurrent(mapper);
				} else {
					updateCurrent(mapper);
				}
			} else {
				if (entitySpec.isStructuralChange(update)) {
					boolean forMainKeyOnlyFalse = false;
					buildDownlinks(forMainKeyOnlyFalse, null, mapper);
					deleteCurrent(mapper);
					putCurrent(mapper);
				} else {
					boolean forMainKeyOnlyFalse = true;
					buildDownlinks(forMainKeyOnlyFalse, null, mapper);
					updateCurrent(mapper);
				}
			}
			
		} else {
			throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
		}
	}
	
	private void updateCurrent(JsonMapper mapper) {
		//table entity
		//this.entity = (E) entity;
		String entityJson = mapper.serialize(this.entity);
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		//B3Update update = new B3Update(B3Table.Entity, entityKey,
		//		new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		DynamoWorker.update(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		
		//main table / lookup / link
		if (entitySpec.mainTable == null) {
			return;
		}
		//buildDownlinks(true, null, mapper);
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		
		//put main entity to main table
		//update = new B3Update(entitySpec.mainTable, mainKey, new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		DynamoWorker.update(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
	}
	
	private void putCurrent(JsonMapper mapper) {
		
		//table entity
		//this.entity = (E) entity;
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		//B3Update put = new B3Update(B3Table.Entity, entityKey,
		//		new B3CellString(B3Table.CELL_LOCATOR_THIZ, mapper.serialize(this.entity)));
		DynamoWorker.put(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
						new B3CellString(B3Table.CELL_LOCATOR_THIZ, mapper.serialize(this.entity)));
		
		//main table / lookup / link
		if (entitySpec.mainTable == null) {
			return;
		}
		//buildDownlinks(false, null, mapper);
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		//put linked entities to table lookup, link
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		boolean noActualPutsFalse = false;
		InitialDumpDeployer.putToLookupAndLinkRecursively(
				entitySpec.mainTable, mainKey, b3Cells, null, this, noActualPutsFalse, null, mapper);
		
		//put main entity to main table
		//B3Update update = new B3Update(entitySpec.mainTable, mainKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), 
				b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void putRevision(String createTime, JsonMapper mapper) {
		//main table / lookup / link
		if (entitySpec.mainTable == null) {
			return;
		}
		//this.entity = (E) targetEntity;
		//buildDownlinks(false, null, mapper);
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		//put linked entities to table lookup, link
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		boolean noActualPutsTrue = true;
		InitialDumpDeployer.putToLookupAndLinkRecursively(entitySpec.mainTable, 
				mainKey, b3Cells, null, this, noActualPutsTrue, null, mapper);
		
		//put main entity to main table
		String revisionId = getRevisionId();
		if (revisionId == null) {
			revisionId = createTime;
		}
		mainKey.setRevisionId(revisionId);
		//B3Update update = new B3Update(entitySpec.mainTable, mainKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), 
				b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void deleteCurrent(JsonMapper mapper) {
		if (this.entity == null) {
			System.out.println("Ignoring entity delete: entity does not exist");
			return;
		}
		
		//delete in table entity
		B3Key key = new B3KeyEntity(this.entity);
		DynamoWorker.delete(B3Table.Entity, key.getHashKey(), key.getRangeKey());
		
		//delete in main table
		if (entitySpec.mainTable != null) {
			key = createMainKey();
			if (key == null) {
				Thread.dumpStack();
				return;
			}
			DynamoWorker.delete(entitySpec.mainTable, key.getHashKey(), key.getRangeKey());
		}
		
		//delete in lookup
		for (B3Table oneMain : B3Table.mainTables) {
			deleteCurrentLookup(oneMain);
		}
		
		//delete in link
		EntityLink[] linkedEntities = getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				B3KeyLink linkKey = new B3KeyLink(
						link.linkedEntityClazz, link.linkedEntityId, entity, link.name);
				DynamoWorker.delete(B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
			}
		}
	}
	
	private void deleteCurrentLookup(B3Table mainTable) {
		B3KeyLookup lookupKey = new B3KeyLookup(entity.getClass(), entity.getId(), mainTable);
		IteratorSupport<Item, QueryOutcome> it = DynamoWorker.query(B3Table.Lookup, lookupKey.getHashKey()).iterator();
		while (it.hasNext()) {
			Item item = it.next();
			DynamoWorker.delete(B3Table.Lookup, lookupKey.getHashKey(), item.getString(DynamoWorker.RANGE));
		}
	}

	/*@SuppressWarnings("unchecked")
	final void applyChangeCreate(EntityCreateWrapper create, ModelShortName entitySpec, JsonMapper mapper) {
		
		//table entity
		this.entity = (E) create.getEntity();
		if (entitySpec.mainTable == null) {
			return;
		}
		System.out.println("CREATE: " + create);
		buildDownlinks(null, mapper);
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			return;
		}
		
		//put linked entities to table lookup, link
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToMainAndLookupAndLinkRecursively(entitySpec.mainTable, 
				mainKey, b3Cells, null, this, null, mapper);
		
		//put main entity to main table
		B3Update update = new B3Update(entitySpec.mainTable, mainKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(update);
	}*/
	
	/*@SuppressWarnings("unchecked")
	final void applyChangeUpdate(EntityUpdateWrapper update, ModelShortName entitySpec, JsonMapper mapper) {
		
		//retrieve entity
		System.out.println("UPDATE: " + update);
		B3KeyEntity entityKey = new B3KeyEntity(update.getEntityClassName(), update.getEntityId());
		Entity targetEntity = entityKey.load(mapper);
		if (targetEntity == null) {
			System.out.println("Ignoring entity update: entity does not exist: " + 
					update.getEntityClassName() + "@" + update.getEntityId());
			return;
		}

		//apply changes
		System.out.println("BEFORE: " + this.entity);
		update.applyChanges(targetEntity);
		System.out.println("AFTER: " + this.entity);
		
		this.entity = (E) targetEntity;
		buildDownlinks(null, mapper);
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			return;
		}
		
		//put linked entities to table lookup, link
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToMainAndLookupAndLinkRecursively(entitySpec.mainTable, 
				mainKey, b3Cells, null, this, null, mapper);
		
		//put main entity to main table
		B3Update b3update = new B3Update(entitySpec.mainTable, mainKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(b3update);
		
		/*LinkedList<String> idPropertyNames = new LinkedList<>();
		EntityLink[] links = new B3EventInfo().getDownlinkedEntities();
		for (EntityLink one : links) {
			idPropertyNames.add(one.getLinkName());
		}
		boolean idChanged = false;
		for (String changedPropertyName : update.getPropertyNames()) {
			if (idPropertyNames.contains(changedPropertyName)) {
				idChanged = true;
			}
		}
		
		if (idChanged) {
			applyChangeUpdateId(update, mapper);
		} else {
			applyChangeUpdateScalar(update, mapper);
		}/
		//applyChangeUpdateScalar(update, entityKey, mapper);
	}*/
	
	/*void applyChangeUpdateScalar(EntityUpdateWrapper update, B3KeyEntity  entityKey, JsonMapper mapper) {

		//apply changes
		System.out.println("BEFORE: " + this.entity);
		update.applyChanges(this.entity);
		System.out.println("AFTER: " + this.entity);
		
		//update table entity
		B3Update put = new B3Update(B3Table.Entity, entityKey,
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, mapper.serialize(this.entity)));
		DynamoWorker.put(put);
	}*/

	/**
	 * Tables to consider: entity, lookup, link, mains
	 * 
	 *   For a value property change: update table entity, mains
	 *   For a linked ID change: update table entity, mains, link, lookup
	 * 
	 * @param update
	 * @param mapper
	 */
	/*void applyChangeUpdateId(EntityUpdateWrapper update, JsonMapper mapper) {


	}*/

	/*final void applyChangeDelete(EntityDeleteWrapper delete, JsonMapper mapper) {
		
	}*/

}
