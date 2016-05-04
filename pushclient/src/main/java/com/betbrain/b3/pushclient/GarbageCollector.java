package com.betbrain.b3.pushclient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.betbrain.b3.data.B3Table;
import com.betbrain.b3.data.DynamoWorker;

@Deprecated
public class GarbageCollector {
	
	/*public static void main(String[] args) {

		final int threads = Integer.parseInt(args[0]);
		DynamoWorker.initialize();
		ModelShortName.initialize();
		
		final B3Bundle bundle = DynamoWorker.getBundleByStatus(DynamoWorker.BUNDLE_STATUS_GARBAGE);
		if (bundle == null) {
			System.out.println("Found no bundles for depoying");
			return;
		}
		DynamoWorker.setBundleStatus(DynamoWorker.BUNDLE_STATUS_DELETING);

		final LinkedList<Runnable> runners = new LinkedList<Runnable>();
		for (int dist = 0; dist < B3Table.DIST_FACTOR; dist++) {
			final int distFinal = dist;
			runners.add(new Runnable() {
				public void run() {
					ItemCollection<QueryOutcome> coll = DynamoWorker.query(
							B3Table.SEPC, DynamoWorker.SEPC_INITIAL + distFinal);

					IteratorSupport<Item, QueryOutcome> iter = coll.iterator();
					int itemCount = 0;
					while (iter.hasNext()) {
						Item item = iter.next();
						itemCount++;
						//System.out.println("Entity " + itemCount + ": " + entity);
						if (itemCount % 10000 == 0) {
							System.out.println("Read count: " + itemCount);
						}
					}
				}
			});
		}
		
		for (int i = 0; i < threads; i++) {
			new Thread() {
				public void run() {
					while (true) {
						Runnable oneRunner;
						synchronized (runners) {
							System.out.println("Remaining runners: " + runners.size());
							if (runners.isEmpty()) {
								return;
							}
							oneRunner = runners.remove();
						}
						oneRunner.run();
					}
				}
			}.start();
		}
	}*/
	
	public static void main(String[] args) {
		
		DynamoWorker.initBundleByStatus(DynamoWorker.BUNDLE_STATUS_DELETEWAIT);
		deleteParallel(B3Table.SEPC, 2);
	}
	
	private static void deleteParallel(final B3Table table, int numberOfThreads) {
		
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        
        // Divide DynamoDB table into logical segments
        // Create one task for scanning each segment
        // Each thread will be scanning one segment
        final int totalSegments = numberOfThreads;
        for (int segment = 0; segment < totalSegments; segment++) {
        	
        	final int segmentFinal = segment;
            executor.execute(new Runnable() {
				
				public void run() {
					System.out.println("Deleting " + table.name + " segment " + segmentFinal);
					DynamoWorker.deleteParallel(table, segmentFinal, totalSegments);
				}
			});
        }

        shutDownExecutorService(executor); 
    }

    private static void shutDownExecutorService(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

}
