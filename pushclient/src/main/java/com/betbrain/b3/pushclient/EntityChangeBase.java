package com.betbrain.b3.pushclient;

import com.betbrain.sepc.connector.util.beans.BeanUtil;

public abstract class EntityChangeBase {

	public abstract String getEntityClassName();
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
}
