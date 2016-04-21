package com.betbrain.b3.data.spec;

import java.util.HashMap;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.EventCentricSpec;
import com.betbrain.sepc.connector.sportsmodel.Event;

/**
 * Event
  + Existing IDs: id, typeId, sportId, templateId, promotionId, parentId, parentPartId, 
    venueId, rootPartId, currentPartId
  + Link type: N/A
  + Denormalization method: N/A
  + Table event: (sportId/eventTypeId/EVENT/eventId), EVENT_id, EVENT_typeId,
    EVENT_sportId, EVENT_templateId, EVENT_promotionId, EVENT_parentId, EVENT_parentPartId, 
    EVENT_venueId, EVENT_statusId, EVENT_rootPartId, EVENT_currentPartId, EVENT_entity (json)
    
  + Table lookup: (EVENT/eventId), typeId, sportId, templateId, promotionId 
    parentId, parentPartId, venueId, statusId, rootPartId, currentPartId
    
  + Table relation:
      Insert: NOTHING
      Query: NONEED
 *
 */
public class EventSpec extends EventCentricSpec<Event> {

	public EventSpec() {
		super(Event.class.getName());
	}

	@Override
	protected long getId(Event e) {
		return e.getId();
	}

	@Override
	public B3KeyEvent getB3KeyMain(Event e) {
		return new B3KeyEvent(e.getSportId(), e.getTypeId(), false, e.getId());
	}

	/*@Override
	protected B3KeyEvent getB3KeyLookup(Event e) {
		return new B3KeyLookup(e.getSportId(), e.getTypeId(), false, e.getId());
	}

	@Override
	protected B3KeyEvent getB3KeyRelation(Event e) {
		return null;
	}*/

	@Override
	protected void getAllIDs(Event e, HashMap<String, Long> map) {
		map.put(Event.PROPERTY_NAME_currentPartId, e.getCurrentPartId());
		map.put(Event.PROPERTY_NAME_parentId, e.getParentId());
		map.put(Event.PROPERTY_NAME_parentPartId, e.getParentPartId());
		map.put(Event.PROPERTY_NAME_promotionId, e.getPromotionId());
		map.put(Event.PROPERTY_NAME_rootPartId, e.getRootPartId());
		map.put(Event.PROPERTY_NAME_sportId, e.getSportId());
		map.put(Event.PROPERTY_NAME_statusId, e.getStatusId());
		map.put(Event.PROPERTY_NAME_templateId, e.getTemplateId());
		map.put(Event.PROPERTY_NAME_venueId, e.getVenueId());
		map.put(Event.PROPERTY_NAME_id, e.getId());
		map.put(Event.PROPERTY_NAME_typeId, e.getTypeId());
	}

}
