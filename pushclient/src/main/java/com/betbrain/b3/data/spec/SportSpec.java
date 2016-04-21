package com.betbrain.b3.data.spec;

import java.util.HashMap;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.EventCentricSpec;
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
public class SportSpec extends EventCentricSpec<Sport> {

	public SportSpec() {
		super(Sport.class.getName());
	}

	@Override
	protected long getId(Sport e) {
		return e.getId();
	}

	@Override
	protected B3KeyEvent getB3KeyMain(Sport e) {
		return new B3KeyEvent(e.getId(), null, null, null);
	}

	@Override
	protected void getAllIDs(Sport e, HashMap<String, Long> map) {
		map.put(Sport.PROPERTY_NAME_parentId, e.getParentId());
		map.put(Sport.PROPERTY_NAME_id, e.getId());
	}

}
