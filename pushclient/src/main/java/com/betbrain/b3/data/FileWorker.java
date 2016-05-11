package com.betbrain.b3.data;

import com.betbrain.b3.pushclient.JsonMapper;

class FileWorker implements DBTrait {
	
	private final JsonMapper mapper;
	
	FileWorker(JsonMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public void put(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		DynamoWorker.putFile(mapper, table, hashKey, rangeKey, cells);
	}

	@Override
	public void update(B3Table table, String hashKey, String rangeKey, B3Cell<?>... cells) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(B3Table table, String hashKey, String rangeKey) {
		throw new UnsupportedOperationException();
	}

}
