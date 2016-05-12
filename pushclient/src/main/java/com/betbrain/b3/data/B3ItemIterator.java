package com.betbrain.b3.data;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;

public class B3ItemIterator {
	
	private final IteratorSupport<Item, QueryOutcome> it;
	
	B3ItemIterator(IteratorSupport<Item, QueryOutcome> it) {
		this.it = it;
	}
	
	public boolean hasNext() {
		if (it == null) {
			return false;
		}
		while (true) {
			try {
				return it.hasNext();
			} catch (ProvisionedThroughputExceededException e) {
				
			}
		}
	}
	
	public Item next() {
		return it.next();
	}
}