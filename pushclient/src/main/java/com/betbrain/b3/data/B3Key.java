package com.betbrain.b3.data;

public abstract class B3Key {
	
	abstract boolean isDetermined();
	
	abstract String getRangeKey();
	
	Integer getHashKey() {
		String r = getRangeKey();
		if (r == null) {
			return null;
		}
		int h = r.hashCode() % 100;
		return Math.abs(h);
	}

}
