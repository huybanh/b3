package com.betbrain.b3.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.b3.data.InitialDumpDeployer;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.b3.pushclient.EntityChangeBase;
import com.betbrain.b3.pushclient.EntityCreateWrapper;
import com.betbrain.b3.pushclient.EntityDeleteWrapper;
import com.betbrain.b3.pushclient.EntityUpdateWrapper;
import com.betbrain.b3.pushclient.JsonMapper;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3Cell;
import com.betbrain.b3.data.B3CellString;
import com.betbrain.b3.data.B3Key;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.B3Update;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Source;

public abstract class B3Entity<E extends Entity/*, K extends B3Key*/> {
	
    //private static final Logger logger = Logger.getLogger(B3Entity.class.getName());

	public E entity;
	
	private ModelShortName entitySpec;
	
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
	
	B3Key createMainKey() {
		return null;
	}
	
	public static void applyChange(EntityChangeBase change, JsonMapper mapper) {
		
		if (Source.class.getName().equals(change.getEntityClassName())) {
			if (change instanceof EntityUpdateWrapper) {
				List<String> changedNames = ((EntityUpdateWrapper) change).getPropertyNames();
				if (changedNames == null || 
						(changedNames.size() == 1 && changedNames.contains(Source.PROPERTY_NAME_lastCollectedTime))) {
					
					//skip this trivial change
					return;
				}
			}
		}
		
		ModelShortName entitySpec = ModelShortName.get(change.getEntityClassName());
		if (entitySpec == null /*|| entitySpec.b3class == null*/) {
			//System.out.println("Ignoring unconfigured change handler " + change.getEntityClassName());
			return;
		}
		
		B3Entity<?> b3entity;
		try {
			b3entity = entitySpec.b3class.newInstance();
			b3entity.entitySpec = entitySpec;
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}

		if (change instanceof EntityCreateWrapper) {
			System.out.println("CHANGE-CREATE: " + change);
			B3KeyEntity entityKey = new B3KeyEntity(change.getEntityClassName(), ((EntityCreateWrapper) change).getEntity().getId());
			b3entity.create(((EntityCreateWrapper) change).getEntity(), entityKey, mapper);
		
		} else if (change instanceof EntityDeleteWrapper) {
			System.out.println("CHANGE-DELETE: " + change);
			EntityDeleteWrapper delete = (EntityDeleteWrapper) change;
			B3KeyEntity entityKey = new B3KeyEntity(delete.getEntityClassName(), delete.getEntityId());
			Entity targetEntity = entityKey.load(mapper);
			if (targetEntity == null) {
				System.out.println("Ignoring entity delete: entity does not exist: " + 
						delete.getEntityClassName() + "/" + delete.getEntityId());
				return;
			}
			b3entity.delete(targetEntity, mapper);	
			
		} else if (change instanceof EntityUpdateWrapper) {
			System.out.println("CHANGE-UPDATE: " + change);
			EntityUpdateWrapper update = (EntityUpdateWrapper) change;
			B3KeyEntity entityKey = new B3KeyEntity(update.getEntityClassName(), update.getEntityId());
			Entity targetEntity = entityKey.load(mapper);
			b3entity.delete(targetEntity, mapper);

			//apply changes
			System.out.println("BEFORE: " + targetEntity);
			update.applyChanges(targetEntity);
			System.out.println("AFTER: " + targetEntity);
			b3entity.create(targetEntity, entityKey, mapper);
			
		} else {
			throw new RuntimeException("Unknown change-wrapper class: " + change.getClass().getName());
		}
	}
	
	@SuppressWarnings("unchecked")
	final void create(Entity entity, B3KeyEntity entityKey, JsonMapper mapper) {
		
		//table entity
		this.entity = (E) entity;
		B3Update put = new B3Update(B3Table.Entity, entityKey,
				new B3CellString(B3Table.CELL_LOCATOR_THIZ, mapper.serialize(this.entity)));
		DynamoWorker.put(put);
		
		//main table / lookup / link
		if (entitySpec.mainTable == null) {
			return;
		}
		buildDownlinks(null, mapper);
		B3Key mainKey = createMainKey();
		//put linked entities to table lookup, link
		LinkedList<B3Cell<?>> b3Cells = new LinkedList<B3Cell<?>>();
		InitialDumpDeployer.putToMainAndLookupAndLinkRecursively(entitySpec.mainTable, 
				mainKey, b3Cells, null, this, null, mapper);
		
		//put main entity to main table
		B3Update update = new B3Update(entitySpec.mainTable, mainKey, b3Cells.toArray(new B3CellString[b3Cells.size()]));
		DynamoWorker.put(update);
	}
	
	final void delete(Entity targetEntity, JsonMapper mapper) {
		if (targetEntity == null) {
			System.out.println("Ignoring entity update: entity does not exist");
			return;
		}
		/*if (this.entity == null) {
			System.out.println("Ignoring entity update: entity does not exist: " + entitySpec.entityClassName + "/" + entity);
			return;
		}*/
		//this.entity = (E) targetEntity;
		//b3entity.applyChangeDelete((EntityDeleteWrapper) change, mapper);
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
