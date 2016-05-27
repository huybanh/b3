package com.betbrain.b3.data;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

public class B3ItemIterator {
	
	private final IteratorSupport<Item, QueryOutcome> it;
	
	//private final ItemCollection<QueryOutcome> coll;
	
	B3ItemIterator(IteratorSupport<Item, QueryOutcome> it, ItemCollection<QueryOutcome> coll) {
		this.it = it;
		//this.coll = coll;
	}
	
	public boolean hasNext() {
		if (it == null) {
			return false;
		}
		while (true) {
			try {
				return it.hasNext();
				/*boolean b = it.hasNext();
				if (!b) {
					System.out.println("consumed capa: " + coll.getAccumulatedConsumedCapacity());
				}
				return b;*/
			} catch (ProvisionedThroughputExceededException e) {
				
			}
		}
	}
	
	public Item next() {
		return it.next();
	}
}