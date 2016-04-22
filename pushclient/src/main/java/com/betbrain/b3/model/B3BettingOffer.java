package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.data.EntityLink;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class B3BettingOffer extends B3Entity<BettingOffer/*, B3KeyOffer*/> {
	
	public B3Provider provider;
	
	public B3Source source;
	
	public B3Outcome outcome;
	
	/*public B3BettingOffer(BettingOffer offer) {
		super(offer);
	}*/

	/*@Override
	public B3KeyOffer getB3KeyMain() {
		
		//sportId, eventTypeId, eventPart, eventId, outcomeTypeId, outcomeId, bettingTypeId, offerId
		return new B3KeyOffer(
				outcome.event.entity.getSportId(),
				outcome.event.entity.getTypeId(),
				false,
				outcome.event.entity.getId(),
				outcome.entity.getTypeId(),
				outcome.entity.getId(),
				this.entity.getBettingTypeId(),
				this.entity.getId());
	}*/

	@Override
	public EntityLink[] getDownlinkedEntities() {
		return new EntityLink[] {new EntityLink(BettingOffer.PROPERTY_NAME_outcomeId, outcome)};
	}
	
	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap) {
		
		/*Entity one = masterMap.get(Outcome.class.getName()).get(entity.getOutcomeId());
		this.outcome = new B3Outcome((Outcome) one);
		this.outcome.buildDownlinks(masterMap);
		
		one = masterMap.get(Provider.class.getName()).get(entity.getProviderId());
		this.provider = new B3Provider((Provider) one);
		this.provider.buildDownlinks(masterMap);*/
		
		this.provider = build(entity.getProviderId(), new B3Provider(), masterMap);
		this.source = build(entity.getSourceId(), new B3Source(), masterMap);
		this.outcome = build(entity.getOutcomeId(), new B3Outcome(), masterMap);
	}
}

class EntityBuilder<E> {

	
}
