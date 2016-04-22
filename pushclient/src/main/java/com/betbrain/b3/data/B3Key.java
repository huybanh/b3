package com.betbrain.b3.data;

public abstract class B3Key {
	
	abstract boolean isDetermined();
	
	abstract Integer getHashKey();
	
	abstract String getRangeKey();

}
