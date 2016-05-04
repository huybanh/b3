package com.betbrain.b3.model;

import java.util.HashMap;

import com.betbrain.b3.pushclient.JsonMapper;
import com.betbrain.sepc.connector.sportsmodel.BettingOffer;
import com.betbrain.sepc.connector.sportsmodel.BettingOfferStatus;
import com.betbrain.sepc.connector.sportsmodel.BettingType;
import com.betbrain.sepc.connector.sportsmodel.Entity;
import com.betbrain.sepc.connector.sportsmodel.Outcome;
import com.betbrain.sepc.connector.sportsmodel.Provider;
import com.betbrain.sepc.connector.sportsmodel.Source;

public class B3BettingOffer extends B3Entity<BettingOffer/*, B3KeyOffer*/> {
	
	public B3Provider provider;
	
	public B3Source source;
	
	public B3Outcome outcome;
	
	public B3BettingType bettingType;
	
	public B3BettingOfferStatus status;

	@Override
	public void getDownlinkedEntitiesInternal() {
		
		//unfollowed links
		addDownlink(BettingOffer.PROPERTY_NAME_outcomeId, Outcome.class, entity.getOutcomeId());
		
		addDownlink(BettingOffer.PROPERTY_NAME_providerId, provider);
		addDownlink(BettingOffer.PROPERTY_NAME_sourceId, source);
		addDownlink(BettingOffer.PROPERTY_NAME_bettingTypeId, bettingType);
		addDownlink(BettingOffer.PROPERTY_NAME_statusId, status);
	}
	
	@Override
	public void buildDownlinks(HashMap<String, HashMap<Long, Entity>> masterMap, JsonMapper mapper) {
		
		this.provider = build(entity.getProviderId(), new B3Provider(), 
				Provider.class, masterMap, mapper);
		this.source = build(entity.getSourceId(), new B3Source(), 
				Source.class, masterMap, mapper);
		this.outcome = build(entity.getOutcomeId(), new B3Outcome(), 
				Outcome.class, masterMap, mapper);
		this.bettingType = build(entity.getBettingTypeId(), 
				new B3BettingType(), BettingType.class, masterMap, mapper);
		this.status = build(entity.getStatusId(), 
				new B3BettingOfferStatus(), BettingOfferStatus.class, masterMap, mapper);
		
		/*Entity one = masterMap.get(Outcome.class.getName()).get(entity.getOutcomeId());
		this.outcome = new B3Outcome((Outcome) one);
		this.outcome.buildDownlinks(masterMap);
		
		one = masterMap.get(Provider.class.getName()).get(entity.getProviderId());
		this.provider = new B3Provider((Provider) one);
		this.provider.buildDownlinks(masterMap);*/
	}
}
