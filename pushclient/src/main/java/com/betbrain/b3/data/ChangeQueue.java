package com.betbrain.b3.data;

import java.util.ArrayList;

import com.betbrain.sepc.connector.sportsmodel.Entity;

public class ChangeQueue {
	
	final int queueId;

	final ArrayList<Entity> entities = new ArrayList<>();
	
	ChangeQueue(int id) {
		this.queueId = id;
	}
}
