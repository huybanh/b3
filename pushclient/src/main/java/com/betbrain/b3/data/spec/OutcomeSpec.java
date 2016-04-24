package com.betbrain.b3.data.spec;

import java.util.LinkedList;

import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.b3.data.EntitySpec;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomeStatus;
import com.betbrain.sepc.connector.sportsmodel.OutcomeType;

public class OutcomeSpec extends EntitySpec<Outcome, B3KeyOffer> {

	public OutcomeSpec() {
		super(B3Table.BettingOffer, Outcome.class.getName());
	}

	@Override
	public B3KeyOffer getB3KeyMain(Outcome e) {
		return null;
	}

	/*@Override
	public B3KeyOffer getB3KeyMainInitially(Outcome outcome, 
			HashMap<String, HashMap<Long, Entity>> masterMap,
			HashMap<Long, Long> eventPartToEventMap) {
		
		HashMap<Long, Entity> allOutcomeTypes = masterMap.get(OutcomeType.class.getName());
		OutcomeType outcomeType = (OutcomeType) allOutcomeTypes.get(outcome.getTypeId());
		Boolean b = outcomeType.getHasParamEventPartId1();
		HashMap<Long, Entity> allEvents = masterMap.get(Event.class.getName());
		if (b == null || !b) {
			Event event = (Event) allEvents.get(outcome.getEventId());
			return new B3KeyOffer(event.getSportId(), event.getTypeId(), false, event.getId(),
					outcome.getTypeId(), outcome.getId());
		} else {
			HashMap<Long, Entity> allEventParts = masterMap.get(EventPart.class.getName());
			EventPart eventPart = (EventPart) allEventParts.get(outcome.getEventPartId());
			while (eventPart.getParentId() != null) {
				eventPart = (EventPart) allEventParts.get(eventPart.getParentId());
			}
			long eventId = eventPartToEventMap.get(eventPart.getId());
			Event event = (Event) allEvents.get(eventId);
			return new B3KeyEvent(event.getSportId(), event.getTypeId(), true, eventPart.getId());
		}
	}*/

	@Override
	protected void getAllDownlinks(Outcome e, LinkedList<EntityLink> downlinks) {
		downlinks.add(new EntityLink(Outcome.PROPERTY_NAME_eventId, e.getEventId(), Event.class));
		downlinks.add(new EntityLink(Outcome.PROPERTY_NAME_eventPartId, e.getEventPartId(), EventPart.class));
		downlinks.add(new EntityLink(Outcome.PROPERTY_NAME_statusId, e.getStatusId(), OutcomeStatus.class));
		downlinks.add(new EntityLink(Outcome.PROPERTY_NAME_typeId, e.getTypeId(), OutcomeType.class));
		
	}

}
