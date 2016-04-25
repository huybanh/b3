package com.betbrain.b3.pushclient;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map.Entry;

import com.betbrain.b3.data.DynamoWorker;
import com.betbrain.b3.data.EntityInitialPutHandler;
import com.betbrain.b3.data.ModelShortName;
import com.betbrain.sepc.connector.sportsmodel.Entity;

public class InitialDumpLocalReader {
	
	private static HashMap<String, HashMap<Long, Entity>> masterMap = new HashMap<String, HashMap<Long,Entity>>();
	
	public static void main(String[] args) throws IOException {
		
		FileInputStream fis = new FileInputStream("initial_dump1.json");
		BufferedInputStream in = new BufferedInputStream(fis);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		int count = 0;
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			Entity entity = JsonMapper.DeserializeF(line);
			HashMap<Long, Entity> subMap = masterMap.get(entity.getClass().getName());
			if (subMap == null) {
				subMap = new HashMap<Long, Entity>();
				masterMap.put(entity.getClass().getName(), subMap);
			}
			subMap.put(entity.getId(), entity);
			if (++count % 100000 == 0) {
				System.out.println("Read line: " + count);
			}
		}
		reader.close();
		in.close();
		fis.close();
		System.out.println("Total line count: " + count);
		
		System.out.println("Got all entities in memory");
		for (Entry<String, HashMap<Long, Entity>> entry : masterMap.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue().size());
		}
		
		ModelShortName.initialize();
		DynamoWorker.initialize();
		new EntityInitialPutHandler(masterMap).initialPut();
	}

}
