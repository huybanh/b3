package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.model.B3Event;
import com.betbrain.b3.pushclient.JsonMapper;

public class EventSearch {
	
	private static JsonMapper jsonMapper = new JsonMapper();

	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent();
		//DynamoWorker.initBundleByStatus("SPRINT2");
		
		B3KeyEvent eventKey = new B3KeyEvent(IDs.EVENT_PREMIERLEAGUE, IDs.EVENTTYPE_GENERICMATCH, (String) null);
		//B3KeyEvent eventKey = new B3KeyEvent(null, IDs.EVENTTYPE_GENERICMATCH, (String) null);
		@SuppressWarnings("unchecked")
		ArrayList<B3Event> eventIds = (ArrayList<B3Event>) eventKey.listEntities(false, jsonMapper);
		for (B3Event e : eventIds) {
			//System.out.println(e.entity.getId() + ": " + e.entity);
			System.out.println(e.entity.getId());
		}
	}

}
