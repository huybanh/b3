package com.betbrain.b3.data.spec;

import java.util.LinkedList;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.b3.data.EntitySpec;
import com.betbrain.sepc.connector.sportsmodel.Sport;

/**
 * + Existing IDs: id, parentId + Link type <Event-Sport>: n-1 
 * + Denormalization method: in-place 
 * + Table event: (sportId/eventTypeId* /[EVENT|EVENTPART]/eventId*), 
 *                SPORT_sportId, SPORT_parentId, SPORT_entity (json) 
 * + Table lookup: (SPORT/sportId), parentId 
 * + Table relation: 
 *    Insert: (EVENT/SPORT/sportId/eventId), [no columns] 
 *    Query: (EVENT/SPORT/sportId/eventId*), [no columns]
 */
public class SportSpec extends EntitySpec<Sport, B3KeyEvent> {

	public SportSpec() {
		super(B3Table.Event, Sport.class.getName());
	}

	@Override
	protected B3KeyEvent getB3KeyMain(Sport e) {
		return new B3KeyEvent(e.getId(), null, null, null);
	}

	@Override
	protected void getAllDownlinks(Sport e, LinkedList<EntityLink> downlinks) {
		//ignore parent linking
		//downlinks.add(new EntityLink(Sport.PROPERTY_NAME_parentId, e.getParentId(), Sport.class));
		//downlinks.add(new EntityLink(Sport.PROPERTY_NAME_id, e.getId()); //NOT a link
	}

}
