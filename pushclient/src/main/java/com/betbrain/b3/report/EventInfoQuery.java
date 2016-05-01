package com.betbrain.b3.report;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Event;

public class EventInfoQuery {
	
	public static void main(String[] args) {
		DynamoWorker.initBundleCurrent();

		//query(217410745);
		query(219501132);
	}
	
	private static void query(long eventId) {

		JsonMapper jsonMapper = new JsonMapper();
		
		//event
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, eventId);
		Event event = keyEntity.load(jsonMapper);
		
		//eventinfo - current status
		//B3KeyEventInfo key = new B3KeyEventInfo(event.getSportId(), event.getTypeId(),
		//		false, eventId, 4l, 17442912l);
		B3KeyEventInfo key = new B3KeyEventInfo(1l, 1l, eventId, 4l, 17442912l);
		key.listEntities(jsonMapper);
	}
}
