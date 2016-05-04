package com.betbrain.b3.pushclient;

import java.beans.PropertyDescriptor;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.util.StringUtil;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public class EntityUpdateWrapper extends EntityChangeBase {

	private String entityClassName;
	
	private long entityId;
	
	private List<String> names;
	
	private List<String> stringValues;
	
	private EntityUpdate update;

	//needed for deserialization
	public EntityUpdateWrapper() {
	}

	public EntityUpdateWrapper(EntityUpdate update) {
		this.update = update;
	}

	public void setEntityClassName(String s) {
		this.entityClassName = s;
	}
	
	@Override
	public String getEntityClassName() {
		if (update != null) {
			return update.getEntityClass().getName();
		}
		return entityClassName;
    }
	
	public void setEntityId(long entityId) {
		this.entityId = entityId;
	}
	
	public long getEntityId() {
		if (update != null) {
			return update.getEntityId();
		}
		return entityId;
	}

	public void setPropertyNames(List<String> names) {
		this.names = names;
	}

	public List<String> getPropertyNames() {
		if (update != null) {
			return update.getPropertyNames();
		}
		return names;
	}
	
	public void setPropertyValues(List<String> stringValues) {
		this.stringValues = stringValues;
	}
	
	public List<String> getPropertyValues() {
		if (update != null) {
			LinkedList<String> stringValues = new LinkedList<String>();
			for (Object obj : update.getPropertyValues()) {
				if (obj != null) {
					stringValues.add(obj.toString());
				} else {
					stringValues.add(null);
				}
			}
			return stringValues;
		}
		return this.stringValues;
	}
	
	public void applyChanges(Entity entity) {
		for (int i = 0; i < names.size(); i++) {
			//setProperty(entity, names.get(i), stringValues.get(i));
			String propertyName = names.get(i);
			PropertyDescriptor desc = BeanUtil.getPropertyDescriptor(entity.getClass(), propertyName);
			Object objectValue;
			String stringValue = stringValues.get(i);
			if (stringValue == null) {
				objectValue = null;
			} else {
				objectValue = StringUtil.parseValue(stringValue, desc.getPropertyType());
			}
			BeanUtil.setPropertyValue(entity, propertyName, objectValue);
		}
	}
	
	/*private static void setProperty(Object bean, String propertyName, String stringValue) {
		PropertyDescriptor desc = BeanUtil.getPropertyDescriptor(bean.getClass(), propertyName);
		//Object objectValue = convert(stringValue, desc.getPropertyType());
		Object objectValue = StringUtil.parseValue(stringValue, desc.getPropertyType());
		BeanUtil.setPropertyValue(bean, propertyName, objectValue);
	}*/
	
	/*static <T> T convert(String stringValue, Class<T> valueType) {
		return StringUtil.parseValue(stringValue, valueType);
	}*/

	/*EntityUpdate makeSEObject() {
		try {
			@SuppressWarnings("unchecked")
			Class<? extends Entity> clazz = (Class<? extends Entity>) Class.forName(entityClass);
			LinkedList<Object> objectValues = new LinkedList<Object>();
			for (String v : values) {
				Object obj = mapper.deserializeObject(v);
				objectValues.add(obj);
			}
			return new EntityUpdate(clazz, this.id, names, objectValues);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException();
		}
	}*/

	public static void main(String[] args) {
		
		JsonMapper mapper = new JsonMapper();
		LinkedList<String> names = new LinkedList<String>();
		names.add(Event.PROPERTY_NAME_endTime);
		LinkedList<Object> values = new LinkedList<Object>();
		values.add("x");
		values.add(2);
		LinkedList<Long> longList = new LinkedList<Long>();
		longList.add(3l);
		longList.add(4l);
		values.add(longList);
		EntityUpdate update = new EntityUpdate(Event.class, 9, names, values);
		System.out.println("Original: " + update);
		
		EntityUpdateWrapper wrapper = new EntityUpdateWrapper(update);
		String s = mapper.serialize(wrapper);
		System.out.println("serialized: " + s);
		Object x = mapper.deserialize(s);
		System.out.println("deserialized: " + BeanUtil.toString(x));
		
		System.out.println("SEPC utils");
		//System.out.println(StringUtil.EMPTY_STRING)
		//BeanUtil.setPropertyValue(bean, propertyName, propertyValue);
		//System.out.println(mapper.deserializeObject("9").getClass().getName());
	}
}
