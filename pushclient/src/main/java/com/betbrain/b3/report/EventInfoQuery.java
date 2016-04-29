package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3Bundle;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventInfoType;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeType;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class EventInfoQuery {
	
	public static void main(String[] args) {
		ModelShortName.initialize();
		DynamoWorker.initialize();

		//query(217410745);
		query(219501132);
	}
	
	private static void query(long eventId) {

		B3Bundle bundle = DynamoWorker.getBundleCurrent(); 
		JsonMapper jsonMapper = new JsonMapper();
		
		//event
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, eventId);
		Event event = keyEntity.load(bundle);
		
		//eventinfo - current status
		//B3KeyEventInfo key = new B3KeyEventInfo(event.getSportId(), event.getTypeId(),
		//		false, eventId, 4l, 17442912l);
		B3KeyEventInfo key = new B3KeyEventInfo(1l, 1l, false, eventId, 4l, 17442912l);
		key.listEntities(bundle, jsonMapper);
	}
}
