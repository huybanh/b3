package com.betbrain.b3.report;

import java.util.ArrayList;

import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventInfo;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class BettrainReport {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ModelShortName.initialize();
		DynamoWorker.initialize();
		String SportFilter = "Football";
		//all sports
		ArrayList<Entity> sports = new B3KeyEntity(Sport.class).listEntities();
		Entity SportEntity = null;
		//Sport Filter
		for(Entity e : sports) {
			System.out.println(e.getName());
			if (e.getName().equals(SportFilter)){
				SportEntity = e;
			}
		}
		SportEntity = sports.get(0);
		//Get All Event for sport
		
		ArrayList<Long> ids;
		
		//ids = new B3KeyLink(Sport.class, SportEntity.getId(), Event.class).listLinks();
		ids = new B3KeyLink(Sport.class, SportEntity.getId(), Event.class, "sportId").listLinks();
		ArrayList<Event> lstEvent = B3KeyEntity.load(Event.class, ids);
		
		System.out.println(lstEvent.size());
		
		//all event types
		ArrayList<Entity> events = new B3KeyEntity(Event.class).listEntities();
		System.out.println(events.size());

		//Why all number of event is 49 but number of event where sportid is 1 = 49 
		
		//all event statuses
		//new B3KeyEntity(EventStatus.class).listEntities();
		
		//event to outcome
		//ids = new B3KeyLink(Event.class, 217409474, EventInfo.class).listLinks();
		ids = new B3KeyLink(Event.class, 217409474, Outcome.class, "eventId").listLinks();
		ArrayList<EventInfo> lstEventInfo = B3KeyEntity.load(EventInfo.class, ids);
		for(EventInfo item : lstEventInfo){
			System.out.println(item.toString());
		}

		//outcome to offer
		//new B3KeyLink(Outcome.class, 2890125761l, BettingOffer.class).listLinks();

	}

}
