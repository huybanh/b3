package com.betbrain.b3.model;

import java.util.ArrayList;

import com.betbrain.b3.pushclient.EntityChangeBase;

public class B3ChangeBatch {

	public final long batchId;
	
	public final ArrayList<EntityChangeBase> changes = new ArrayList<>();
	
	public B3ChangeBatch(long batchId) {
		this.batchId = batchId;
	}
}
