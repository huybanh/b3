package com.betbrain.b3.data;

abstract class B3Key {
	
	final String SEP = "/";
	
	abstract boolean isDetermined();
	
	abstract Integer getHashKey();
	
	abstract String getRangeKey();

}
