package com.betbrain.b3.report;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.betbrain.b3.data.B3ItemIterator;
import com.betbrain.b3.data.B3KeyEntity;
import com.betbrain.b3.data.B3KeyEventInfo;
import com.betbrain.b3.data.B3KeyLink;
import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.Sport;

public class EventInfoQuery{
	
	private static JsonMapper mapper = new JsonMapper();
	
	public static void main(String[] args) {
		
		DynamoWorker.initBundleCurrent(); 
		
		B3KeyLink keyLink = new B3KeyLink(Sport.class, IDs.SPORT_FOOTBALL, Event.class, Event.PROPERTY_NAME_sportId);
		B3ItemIterator it = DynamoWorker.query(B3Table.Link, keyLink.getHashKey());
		int count = 0;
		while (it.hasNext()) {
			Item item = it.next();
			long eventId = Long.parseLong(item.getString(DynamoWorker.RANGE));
			queryEvent(eventId);
			count++;
			if (count % 100 == 0) {
				System.out.println("Queried events " + count + ": " + havingOfferCount + "/" + havingInfoCount);
			}
		}
	}
	
	private static int havingOfferCount, havingInfoCount;
	
	private static void queryEvent(long eventId) {
		
		B3KeyEntity keyEntity = new B3KeyEntity(Event.class, eventId);
		Event e = keyEntity.load(mapper);
		
		//offers
		B3KeyOffer offerKey = new B3KeyOffer(e.getSportId(), e.getTypeId(), eventId);
		B3ItemIterator it = DynamoWorker.query(B3Table.BettingOffer, offerKey.getHashKey());
		/*int offerCount = 0;
		/*while (it.hasNext()) {
			it.next();
			offerCount++;
		}
		if (offerCount > 0) havingOfferCount++;*/
		if (it.hasNext()) {
			havingOfferCount++;
		}

		//info
		B3KeyEventInfo infoKey = new B3KeyEventInfo(e.getSportId(), e.getTypeId(), eventId);
		it = DynamoWorker.query(B3Table.EventInfo, infoKey.getHashKey());
		/*int infoCount = 0;
		while (it.hasNext()) {
			it.next();
			infoCount++;
		}
		if (infoCount > 0) havingInfoCount++;*/
		if (it.hasNext()) {
			havingInfoCount++;
		}
		
		/*if (offerCount > 0 && infoCount > 0) {
			System.out.println("Event " + eventId + ":  offer count: " + offerCount + ", info count: " + infoCount);
		}*/
		
		/*B3KeyLink keyLink = new B3KeyLink(Event.class, eventId, Outcome.class, "eventId");
		ArrayList<Long> outcomeIds = keyLink.listLinks();
		ArrayList<Long> offerIds;
		if (!outcomeIds.isEmpty()) {
			keyLink = new B3KeyLink(Outcome.class, outcomeIds.get(0), BettingOffer.class, "outcomeId");
			offerIds = keyLink.listLinks();
		} else {
			offerIds = new ArrayList<Long>();
		}
		System.out.println("Event " + eventId + ": outcome count: " + + outcomeIds.size() + ", offer count: " + offerIds.size());*/
		//ids = new B3KeyLink(sports.get(0), Event.class).listLinks();
		//B3KeyEntity.load(Event.class, ids);
		
		//all event types
		//new B3KeyEntity(EventType.class).listEntities();
		
		//all event statuses
		//new B3KeyEntity(EventStatus.class).listEntities();

		//new B3KeyEntity(OutcomeType.class).listEntities(bundle, jsonMapper);
		//new B3KeyEntity(EventInfoType.class).listEntities();
		//new B3KeyEntity(EventInfo.class).listEntities();
		//B3KeyEntity.load(EventInfoType.class, 92);
		
		//event to outcome
		//ids = new B3KeyLink(Event.class, 217409474, Outcome.class).listLinks();
		//B3KeyEntity.load(Outcome.class, ids);
		
		//outcome to offer
		//new B3KeyLink(Outcome.class, 2890125761l, BettingOffer.class).listLinks();
	}
}
