package com.betbrain.b3.data;

import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.EntityUpdate;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.util.StringUtil;
import com.betbrain.sepc.connector.util.beans.BeanUtil;

public class ChangeUpdateWrapper extends ChangeBase {

	private String entityClassName;
	
	private long entityId;
	
	private List<String> names;
	
	private List<String> stringValues;
	
	private EntityUpdate update;

	//needed for deserialization
	public ChangeUpdateWrapper() {
	}

	public ChangeUpdateWrapper(EntityUpdate update) {
		this.update = update;
	}
	
	public String validate() {
		try {
			if (update.getPropertyNames().size() != update.getPropertyValues().size()) {
				return "Update with different numbers of names and values";
			}
		} catch (NullPointerException ne) {
			return "Update with null lists of names/values";
		}
		return null;
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

	public void setB3PropertyNames(List<String> names) {
		this.names = names;
	}

	public List<String> getB3PropertyNames() {
		if (update != null) {
			return update.getPropertyNames();
		}
		return names;
	}
	
	public void setB3PropertyValues(List<String> stringValues) {
		this.stringValues = stringValues;
	}
	
	public List<String> getB3PropertyValues() {
		if (update != null) {
			LinkedList<String> stringValues = new LinkedList<String>();
			for (Object obj : update.getPropertyValues()) {
				if (obj != null) {
					stringValues.add(obj.toString());
				} else {
					stringValues.add("NULL");
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
			/*if (stringValue == null) {
				objectValue = null;
			} else {
				objectValue = StringUtil.parseValue(stringValue, desc.getPropertyType());
			}*/
			//hack flexjson: force nulls in list
			if ("NULL".equals(stringValue)) {
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
		List<String> names = new LinkedList<String>();
		names.add(Event.PROPERTY_NAME_endTime);
		List<Object> values = new LinkedList<Object>();
		values.add("x");
		values.add(2);
		LinkedList<Long> longList = new LinkedList<Long>();
		longList.add(3l);
		longList.add(4l);
		//values.add(longList);
		EntityUpdate update = new EntityUpdate(Event.class, 9, names, values);
		System.out.println("Original: " + update);
		
		ChangeUpdateWrapper wrapper = new ChangeUpdateWrapper(update);
		String s = mapper.serialize(wrapper);
		System.out.println("serialized: " + s);
		Object x = mapper.deserialize(s);
		System.out.println("deserialized: " + BeanUtil.toString(x));
		
		System.out.println("Testing nulls");
		//System.out.println(StringUtil.EMPTY_STRING)
		//BeanUtil.setPropertyValue(bean, propertyName, propertyValue);
		//System.out.println(mapper.deserializeObject("9").getClass().getName());
		
		names = Arrays.asList("paramFloat1", "paramParticipantId1", "paramString1");
		LinkedList<Object> valuesWithNulls = new LinkedList<>();
		valuesWithNulls.add(null);
		valuesWithNulls.add(null);
		valuesWithNulls.add("Injury");
		EntityUpdate u = new EntityUpdate(EventInfo.class, 1L, names, valuesWithNulls);
		System.out.println(u);
		wrapper = new ChangeUpdateWrapper(u);
		s = new JsonMapper().serialize(wrapper);
		System.out.println(s);
		Object o = new JsonMapper().deserialize(s);
		System.out.println(o);
	}
}
