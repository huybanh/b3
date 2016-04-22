package com.betbrain.b3.data.spec;

import java.util.LinkedList;

import com.betbrain.b3.data.B3KeyEvent;
import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.EntityLink;
import com.betbrain.b3.data.EntitySpec;
import com.betbrain.sepc.connector.sportsmodel.Event;
import com.betbrain.sepc.connector.sportsmodel.EventPart;
import com.betbrain.sepc.connector.sportsmodel.EventStatus;
import com.betbrain.sepc.connector.sportsmodel.EventTemplate;
import com.betbrain.sepc.connector.sportsmodel.EventType;
import com.betbrain.sepc.connector.sportsmodel.Sport;

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
public class EventSpec extends EntitySpec<Event, B3KeyEvent> {

	public EventSpec() {
		super(B3Table.Event, Event.class.getName());
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
	protected void getAllDownlinks(Event e, LinkedList<EntityLink> downlinks) {
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_currentPartId, e.getCurrentPartId(), EventPart.class));
		//ignore parent linking
		//downlinks.add(new EntityLink(Event.PROPERTY_NAME_parentId, e.getParentId(), Event.class));
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_parentPartId, e.getParentPartId(), EventPart.class));
		//downlinks.add(new EntityLink(Event.PROPERTY_NAME_promotionId, e.getPromotionId(), promotion);
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_rootPartId, e.getRootPartId(), EventPart.class));
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_sportId, e.getSportId(), Sport.class));
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_statusId, e.getStatusId(), EventStatus.class));
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_templateId, e.getTemplateId(), EventTemplate.class));
		//downlinks.add(new EntityLink(Event.PROPERTY_NAME_venueId, e.getVenueId(), ); //no entity class found
		//downlinks.add(new EntityLink(Event.PROPERTY_NAME_id, e.getId(), ); //NOT a link
		downlinks.add(new EntityLink(Event.PROPERTY_NAME_typeId, e.getTypeId(), EventType.class));
	}

}
