package cloud.developing.dynamodb.ttl;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

/**
 * @author asmolnik
 *
 */
public class Main {

	public static void main(String[] args) throws InterruptedException {
		ExecutorService es = null;
		try {
			es = Executors.newFixedThreadPool(10);
			DynamoDB db = new DynamoDB(new AmazonDynamoDBClient());
			Table t = db.getTable("active-sessions");
			int recordCount = 10_000;
			CountDownLatch latch = new CountDownLatch(recordCount);
			AtomicInteger counter = new AtomicInteger();
			for (int i = 0; i < recordCount; i++) {
				es.submit(() -> {
					ThreadLocalRandom random = ThreadLocalRandom.current();
					int id = counter.incrementAndGet();
					int expirationTime = random.nextInt(30 * 60) + 20 * 60;
					t.putItem(new Item().withPrimaryKey("session-id", "session-id-" + id).with("session-data", "nothing-special").with("ttl",
							Instant.now().getEpochSecond() + expirationTime));
					latch.countDown();
					System.out.println(id);
				});
			}
			latch.await();
		} finally {
			if (es != null) {
				es.shutdown();
			}
		}

	}
	
}
