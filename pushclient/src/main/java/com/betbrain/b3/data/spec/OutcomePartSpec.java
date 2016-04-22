package com.betbrain.b3.data.spec;

import java.util.LinkedList;

import com.betbrain.b3.data.B3KeyOffer;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.b3.data.EntitySpec;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.OutcomePart;
import com.betbrain.sepc.connector.sportsmodel.OutcomePartition;

public class OutcomePartSpec extends EntitySpec<OutcomePart, B3KeyOffer> {

	public OutcomePartSpec() {
		super(B3Table.BettingOffer, OutcomePart.class.getName());
	}

	@Override
	public B3KeyOffer getB3KeyMain(OutcomePart e) {
		//return new B3KeyEvent(e.getSportId(), e.getTypeId(), false, e.getId());
		return null;
	}

	/*@Override
	public B3KeyOffer getB3KeyMainInitially(OutcomePart e, 
			HashMap<String, HashMap<Long, Entity>> masterMap,
			HashMap<Long, Long> eventPartToEventMap) {
		
		HashMap<Long, Entity> allOutcomes = masterMap.get(Outcome.class.getName());
		Outcome outcome = (Outcome) allOutcomes.get(e.getOutcomeId());
		HashMap<Long, Entity> allOutcomeTypes = masterMap.get(OutcomeType.class.getName());
		OutcomeType outcomeType = (OutcomeType) allOutcomeTypes.get(outcome.getTypeId());
		Boolean b = outcomeType.getHasParamEventPartId1();
		HashMap<Long, Entity> allEvents = masterMap.get(Event.class.getName());
		if (b == null || !b) {
			Event event = (Event) allEvents.get(outcome.getEventId());
			return new B3KeyEvent(event.getSportId(), event.getTypeId(), false, event.getId());
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
	protected void getAllDownlinks(OutcomePart e, LinkedList<EntityLink> downlinks) {
		downlinks.add(new EntityLink(OutcomePart.PROPERTY_NAME_outcomeId, e.getOutcomeId(), Outcome.class));
		downlinks.add(new EntityLink(OutcomePart.PROPERTY_NAME_outcomePartitionId, e.getOutcomePartitionId(), OutcomePartition.class));
	}

}
