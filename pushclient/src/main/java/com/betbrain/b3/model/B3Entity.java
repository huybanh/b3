package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.data.EntitySpec2;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3Cell;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.ChangeSet;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityChange;
import com.betbrain.sepc.connector.sportsmodel.EntityCreate;
import com.betbrain.sepc.connector.sportsmodel.EntityDelete;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
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
	
	/**
	 * @return error message if cannot, null if ok
	 */
	String canCreateMainKey() {
		return null; // yes, we can, no error returned
	}
	
	B3Key createMainKey() {
		return null;
	}
	
	String getRevisionId() {
		throw new RuntimeException();
	}
	
	private void updatePropertyValues(EntityUpdate update) {
		List<String> names = update.getPropertyNames();
		List<Object> values = update.getPropertyValues();
		for (int index = 0; index < names.size(); index++) {
			String propertyName = names.get(index);
			Object propertyValue = values.get(index);
			BeanUtil.setPropertyValue(entity, propertyName, propertyValue);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void applyChange(ChangeSet changeSet, EntityChange change, long changeTime,
			HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {

		//pre-apply
		//this.entity = (E) change.lookupEntity(masterMap);
		EntitySpec2 entitySpec = getSpec();
		boolean onlyEntityMainIDsNeeded;
		if (change instanceof EntityCreate) {
			this.entity = (E) ((EntityCreate) change).getEntity();
			onlyEntityMainIDsNeeded = false;
		} else {
			long entityId;
			if (change instanceof EntityUpdate) {
				entityId = ((EntityUpdate) change).getEntityId();
				if (entitySpec.revisioned) {
					onlyEntityMainIDsNeeded = false;
				} else {
					onlyEntityMainIDsNeeded = !entitySpec.isStructuralChange(((EntityUpdate) change));
				}
			} else {
				//change delete
				entityId = ((EntityDelete) change).getEntityId();
				onlyEntityMainIDsNeeded = true;
			}
			this.entity = (E) masterMap.get(change.getEntityClass().getName()).get(entityId);

			if (this.entity == null) {
				DynamoWorker.logError("Got UPDATE/DELETE, but no entity found: " + 
						change.getEntityClass().getName() + "/" + entityId);
				return;
				//System.out.println("Err: " + change);
				//System.out.println("Err: " + this.entity);
				//throw new RuntimeException();
			}
		}
		
		if (change instanceof EntityUpdate) {
			//EntityUpdate update = (EntityUpdate) change;
			//update.applyChanges(this.entity);
			updatePropertyValues((EntityUpdate) change);
		}
		//boolean forMainKeyOnly = change.isOnlyEntityMainIDsNeeded(getSpec());
		buildDownlinks(onlyEntityMainIDsNeeded, masterMap, null);
		String error = canCreateMainKey();
		if (entitySpec.mainTable != null && error != null) {
			DynamoWorker.logError("Got " + change + ", but " + error);
			return;
			//System.out.println("Err2: " + change);
			//System.out.println("Err2: " + this.entity);
			//throw new RuntimeException();
		}
		
		//post-apply
		if (change instanceof EntityCreate) {
			
			System.out.println(Thread.currentThread().getName() + " CHANGE-CREATE: " + change);
			B3Key mainKey = createMainKey();
			LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
			InitialDumpDeployer.putToLookupAndLinkRecursively(
					changeSet, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
			B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
			String entityJson = mapper.serialize(this.entity);
			
			putCurrent(changeSet, mainKey, cellArray, entityJson);
			masterMap.get(this.entity.getClass().getName()).put(this.entity.getId(), this.entity);
			System.out.println("New entity created: " + this.getSpec().shortName + ": " + this.entity.getId());
			if (entitySpec.revisioned) {
				putRevision(changeSet, changeTime, mainKey, cellArray);
			}
		
		} else if (change instanceof EntityDelete) {
			System.out.println(Thread.currentThread().getName() + " CHANGE-DELETE: " + change);
			deleteCurrent(changeSet);
			masterMap.get(this.entity.getClass().getName()).remove(this.entity.getId());
			
		} else if (change instanceof EntityUpdate) {
			
			System.out.println(Thread.currentThread().getName() + " CHANGE-UPDATE: " + change);
			//ChangeUpdateWrapper update = (ChangeUpdateWrapper) change;
			if (entitySpec.isStructuralChange(((EntityUpdate) change))) {

				B3Key mainKey = createMainKey();
				LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
				InitialDumpDeployer.putToLookupAndLinkRecursively(
						changeSet, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
				B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
				String entityJson = mapper.serialize(this.entity);

				deleteCurrent(changeSet);
				putCurrent(changeSet, mainKey, cellArray, entityJson);
				
				if (entitySpec.revisioned) {
					putRevision(changeSet, changeTime, mainKey, cellArray);
				}
			} else {
				String entityJson = mapper.serialize(this.entity);
				updateCurrent(changeSet, entityJson);
				if (entitySpec.revisioned) {

					B3Key mainKey = createMainKey();
					LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
					InitialDumpDeployer.putToLookupAndLinkRecursively(
							changeSet, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);
					B3CellString[] cellArray = b3Cells.toArray(new B3CellString[b3Cells.size()]);
					putRevision(changeSet, changeTime, mainKey, cellArray);
				}
			}
			masterMap.get(this.entity.getClass().getName()).put(this.entity.getId(), this.entity);
			
		} else {
			throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
		}
	}
	
	/*@SuppressWarnings("unchecked")
	public boolean preApplyChange(ChangeBase change, 
			HashMap<String, HashMap<Long, Entity>> masterMap) {

		this.entity = (E) change.lookupEntity(masterMap);
		
		//validate if target entity exists
		//if (!(change instanceof ChangeCreateWrapper)) {
			if (this.entity == null) {
				//DynamoWorker.logError("Got " + change + ", but no entity found: " + 
				//		change.getEntityClassName() + "/" + change.getEntityId());
				//return false;
				System.out.println("Err: " + change);
				System.out.println("Err: " + this.entity);
				System.out.println("Err: " + change.b3entity);
				throw new RuntimeException();
			}
		//}
		if (change instanceof ChangeUpdateWrapper) {
			ChangeUpdateWrapper update = (ChangeUpdateWrapper) change;
			update.applyChanges(this.entity);
		}
		boolean forMainKeyOnly = change.isOnlyEntityMainIDsNeeded(getSpec());
		buildDownlinks(forMainKeyOnly, masterMap, null);
		if (getSpec().mainTable != null && createMainKey() == null) {
			System.out.println("Err2: " + change);
			System.out.println("Err2: " + this.entity);
			System.out.println("Err2: " + change.b3entity);
			throw new RuntimeException();
		}
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
			
		} else {
			throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
		}
	}*/
	
	private void updateCurrent(ChangeSet changeSet, String entityJson) {
		
		//table entity
		//String entityJson = mapper.serialize(this.entity);
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		changeSet.update(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			//enity doesn't have its own main table
			return;
		}
		B3Key mainKey = createMainKey();
		//if (mainKey == null) {
			//something went wrong
			//throw new RuntimeException();
		//}
		
		//put main entity to main table
		changeSet.update(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(),
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
	}
	
	private void putCurrent(ChangeSet changeSet, B3Key mainKey, B3CellString[] cells, String entityJson) {
		
		//table entity
		B3KeyEntity entityKey = new B3KeyEntity(entity.getClass().getName(), entity.getId());
		changeSet.put(B3Table.Entity, entityKey.getHashKey(), entityKey.getRangeKey(),
						new B3CellString(B3Table.CELL_LOCATOR_THIZ, entityJson));
		
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			//enity doesn't have its own main table
			return;
		}
		//B3Key mainKey = createMainKey();
		//if (mainKey == null) {
			//something went wrong
		//	throw new RuntimeException();
		//}
		//put linked entities to table lookup, link
		/*LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToLookupAndLinkRecursively(
				false, entitySpec.mainTable, mainKey, b3Cells, null, this, masterMap, mapper);*/
		
		//put main entity to main table
		changeSet.put(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), cells);
				//b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void putRevision(ChangeSet changeSet, long changeTime, B3Key mainKey, B3CellString[] cells) {
		//main table / lookup / link
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable == null) {
			//enity doesn't have its own main table
			return;
		}
		//B3Key mainKey = createMainKey();
		//if (mainKey == null) {
			//something went wrong
			//throw new RuntimeException();
		//}
		//put linked entities to table lookup, link
		/*LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToLookupAndLinkRecursively(false, entitySpec.mainTable, 
				null, //no actual puts 
				b3Cells, null, this, masterMap, mapper);*/
		
		//put main entity to main table
		String revisionId = getRevisionId();
		if (revisionId == null) {
			revisionId = String.valueOf(changeTime);
		}
		mainKey.setRevisionId(revisionId);
		changeSet.put(entitySpec.mainTable, mainKey.getHashKey(), mainKey.getRangeKey(), cells);
				//b3Cells.toArray(new B3CellString[b3Cells.size()]));
	}
	
	private void deleteCurrent(ChangeSet changeSet) {
		if (this.entity == null) {
			System.out.println("Ignoring entity delete: entity does not exist");
			return;
		}
		
		//delete in table entity
		B3Key key = new B3KeyEntity(this.entity);
		changeSet.delete(B3Table.Entity, key.getHashKey(), key.getRangeKey());
		
		//delete in main table
		EntitySpec2 entitySpec = getSpec();
		if (entitySpec.mainTable != null) {
			key = createMainKey();
			//if (key == null) {
				//something went wrong
			//	throw new RuntimeException();
			//}
			changeSet.delete(entitySpec.mainTable, key.getHashKey(), key.getRangeKey());
		}
		
		//delete in lookup
		//for (B3Table oneMain : B3Table.mainTables) {
			//deleteCurrentLookup(oneMain);
		//}
		changeSet.deleteCurrentLookup(entity.getClass(), entity.getId());
		
		//delete in link
		EntityLink[] linkedEntities = getDownlinkedEntities();
		if (linkedEntities != null) {
			for (EntityLink link : linkedEntities) {
				B3KeyLink linkKey = new B3KeyLink(
						link.linkedEntityClazz, link.linkedEntityId, entity, link.name);
				changeSet.delete(B3Table.Link, linkKey.getHashKey(), linkKey.getRangeKey());
			}
		}
	}
	
	/*private void deleteCurrentLookup(ChangeSet changeSet) {
		B3KeyLookup lookupKey = new B3KeyLookup(entity.getClass(), entity.getId());
		B3ItemIterator it = DynamoWorker.query(B3Table.Lookup, lookupKey.getHashKey());
		while (it.hasNext()) {
			Item item = it.next();
			changeSet.delete(B3Table.Lookup, lookupKey.getHashKey(), item.getString(DynamoWorker.RANGE));
		}
	}*/

}
