package com.betbrain.b3.data;

public abstract class B3Key {
	
	abstract boolean isDetermined();
	
	abstract String getRangeKey();
	
	abstract String getHashKey();
	
	/*protected int module(int l, int m) {
		return Math.abs(l % m);
	}*/

}
