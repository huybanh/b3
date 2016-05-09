package com.betbrain.b3.data;

import java.util.LinkedList;

import com.betbrain.sepc.connector.util.beans.BeanUtil;

public abstract class ChangeBase {
	
	public String hashKey;
	
	public String rangeKey;
	
	public String changeTime;
	
	public int queueId;
	
	public boolean deployed = false;
	
	private LinkedList<ChangeBase> precedents;

	public abstract String getEntityClassName();
	
	@Override
	public String toString() {
		return BeanUtil.toString(this);
	}
}
