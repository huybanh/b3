package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;

import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.data.ChangeUpdateWrapper;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3Cell;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3ItemIterator;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3KeyLookup;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ChangeBase;
import com.betbrain.b3.data.ChangeCreateWrapper;
import com.betbrain.b3.data.ChangeDeleteWrapper;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public abstract class B3Entity<E extends Entity/*, K extends B3Key*/> {
	
    //private static final Logger logger = Logger.getLogger(B3Entity.class.getName());

	public E entity;
	
	//private EntitySpec2 entitySpec;
	
	private LinkedList<EntityLink> downlinks;
	
	protected B3Entity() {
	}
	
	/*protected B3Entity(E entity) {
		this.entity = entity;
	}*/
	
	//abstract public K getB3KeyMain();
	
	public abstract EntitySpec2 getSpec();
	
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
	
	abstract public void buildDownlinks(boolean forMainKeyOnly, 
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper);
	
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
	
	/*public void setSpec(EntitySpec2 spec) {
		this.entitySpec = spec;
	}*/
	
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
	void deserialize(JsonMapper mapper, Item item, String cellName) {
		String json = item.getString(cellName);
		if (json != null) {
			entity = (E) mapper.deserialize(json);
		}
	}
	
	B3Key createMainKey() {
		return null;
	}
	
	String getRevisionId() {
		throw new RuntimeException();
	}
	
	@SuppressWarnings("unchecked")
	public boolean preApplyChange(ChangeBase change, 
			HashMap<String, HashMap<Long, Entity>> masterMap) {

		this.entity = (E) change.lookupEntity(masterMap);
		
		//validate if target entity exists
		if (!(change instanceof ChangeCreateWrapper)) {
			if (this.entity == null) {
				DynamoWorker.logError("Got " + change + ", but no entity found: " + 
						change.getEntityClassName() + "/" + change.getEntityId());
				return false;
			}
		}
		if (change instanceof ChangeUpdateWrapper) {
			ChangeUpdateWrapper update = (ChangeUpdateWrapper) change;
			update.applyChanges(this.entity);
		}
		boolean forMainKeyOnly = change.needEntityMainIDsOnly(getSpec());
		buildDownlinks(forMainKeyOnly, masterMap, null);
		return true;
	}
	
	public void postApplyChange(ChangeBase change, 
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
		EntitySpec2 entitySpec = getSpec();
		if (change instanceof ChangeCreateWrapper) {
			
			System.out.println(Thread.currentThread().getName() + "CHANGE-CREATE: " + change);
			B3Key mainKey = createMainKey();
			LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
			InitialDumpDeployer.putToLookupAndLinkRecursively(
					false, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
			B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
			String entityJson = mapper.serialize(this.entity);
			
			putCurrent(mainKey, cellArray, entityJson);
			masterMap.get(this.entity.getClass().getName()).put(this.entity.getId(), this.entity);
			if (entitySpec.revisioned) {
				putRevision(change.changeTime, mainKey, cellArray);
			}
		
		} else if (change instanceof ChangeDeleteWrapper) {
			System.out.println(Thread.currentThread().getName() + "CHANGE-DELETE: " + change);
			deleteCurrent();
			masterMap.get(this.entity.getClass().getName()).remove(this.entity.getId());
			
		} else if (change instanceof ChangeUpdateWrapper) {
			
			System.out.println(Thread.currentThread().getName() + "CHANGE-UPDATE: " + change);
			ChangeUpdateWrapper update = (ChangeUpdateWrapper) change;
			if (entitySpec.isStructuralChange(update)) {

				B3Key mainKey = createMainKey();
				LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
				InitialDumpDeployer.putToLookupAndLinkRecursively(
						false, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
				B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
				String entityJson = mapper.serialize(this.entity);

				deleteCurrent();
				putCurrent(mainKey, cellArray, entityJson);
				
				if (entitySpec.revisioned) {
					putRevision(change.changeTime, mainKey, cellArray);
				}
			} else {
				String entityJson = mapper.serialize(this.entity);
				updateCurrent(entityJson);
				if (entitySpec.revisioned) {

					B3Key mainKey = createMainKey();
					LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
					InitialDumpDeployer.putToLookupAndLinkRecursively(
							false, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
					B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
					putRevision(change.changeTime, mainKey, cellArray);
				}
			}
			masterMap.get(this.entity.getClass().getName()).put(this.entity.getId(), this.entity);
			/*if (entitySpec.revisioned) {
				putRevision(change.changeTime, masterMap, mapper);
				if (entitySpec.isStructuralChange(update)) {
					//deleteCurrent(mapper);
					putCurrent(masterMap, mapper);
				} else {
					updateCurrent(mapper);
				}
			} else {
				if (entitySpec.isStructuralChange(update)) {
					//deleteCurrent(mapper);
					putCurrent(masterMap, mapper);
				} else {
					updateCurrent(mapper);
				}
			}
			masterMap.get(this.entity.getClass().getName()).put(this.entity.getId(), this.entity);*/
			
		} else {
			throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
		}
	}
	
	private void updateCurrent(String entityJson) {
		
		//table entity
		//String entityJson = mapper.serialize(this.entity);
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		DynamoWorker.update(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			return;
		}
		B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		
		//put main entity to main table
		DynamoWorker.update(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
	}
	
	private void putCurrent(B3Key mainKey, B3CellString[] cells, String entityJson) {
		
		//table entity
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		DynamoWorker.put(true, B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
						new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			return;
		}
		//B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		//put linked entities to table lookup, link
		/*LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToLookupAndLinkRecursively(
				false, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);*/
		
		//put main entity to main table
		DynamoWorker.put(true, entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), cells);
				//b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void putRevision(String createTime, B3Key mainKey, B3CellString[] cells) {
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			return;
		}
		//B3Key mainKey = createMainKey();
		if (mainKey == null) {
			Thread.dumpStack();
			return;
		}
		//put linked entities to table lookup, link
		/*LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToLookupAndLinkRecursively(false, entitySpec.mainTable, 
				null, //no actual puts 
				b3Cells, null, this, masterMap, mapper);*/
		
		//put main entity to main table
		String revisionId = getRevisionId();
		if (revisionId == null) {
			revisionId = createTime;
		}
		mainKey.setRevisionId(revisionId);
		DynamoWorker.put(true, entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), cells);
				//b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void deleteCurrent() {
		if (this.entity == null) {
			System.out.println("Ignoring entity delete: entity does not exist");
			return;
		}
		
		//delete in table entity
		B3Key key = new B3KeyEntity(this.entity);
		DynamoWorker.delete(B3Table.Entity, key.getHashKey(), key.getRangeKey());
		
		//delete in main table
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable != null) {
			key = createMainKey();
			if (key == null) {
				Thread.dumpStack();
				return;
			}
			DynamoWorker.delete(entitySpec.mainTable, key.getHashKey(), key.getRangeKey());
		}
		
		//delete in lookup
		//for (B3Table oneMain : B3Table.mainTables) {
			//deleteCurrentLookup(oneMain);
		//}
		deleteCurrentLookup();
		
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
	
	private void deleteCurrentLookup() {
		B3KeyLookup lookupKey = new B3KeyLookup(entity.getClass(), entity.getId());
		B3ItemIterator it = DynamoWorker.query(B3Table.Lookup, lookupKey.getHashKey());
		while (it.hasNext()) {
			Item item = it.next();
			DynamoWorker.delete(B3Table.Lookup, lookupKey.getHashKey(), item.getString(DynamoWorker.RANGE));
		}
	}

}
