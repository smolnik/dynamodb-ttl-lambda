package cloud.developing.dynamodb.ttl;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * @author asmolnik
 *
 */
public class SessionExpirationHandler {

	private static final String EVENT_NAME_REMOVE = "REMOVE";

	private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

	public void handle(DynamodbEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		StringBuilder removeLogEntries = event.getRecords().stream().map(r -> {
			String eventName = r.getEventName();
			StreamRecord sr = r.getDynamodb();
			String sessionId = sr.getKeys().get("session-id").getS();
			Instant now = Instant.now();
			logger.log(String.format("%s %s %s", sessionId, eventName, now));
			if (EVENT_NAME_REMOVE.equals(eventName)) {
				long ttl = Long.parseLong(sr.getOldImage().get("ttl").getN());
				long timeToDeleteInSecs = now.getEpochSecond() - ttl;
				long timeToDeleteInMins = timeToDeleteInSecs / 60;
				long timeToDeleteInHours = timeToDeleteInMins / 60;
				return Optional.of(String.format("%s,%d,%d,%d,%s,%s%n", sessionId, timeToDeleteInSecs, timeToDeleteInMins, timeToDeleteInHours, now,
						Instant.ofEpochSecond(ttl)));
			}
			return Optional.empty();
		}).collect(StringBuilder::new, (sb, logEntry) -> {
			if (logEntry.isPresent()) {
				sb.append(logEntry.get());
			}
		}, StringBuilder::append);

		if (removeLogEntries.length() > 0) {
			byte[] bytes = removeLogEntries.toString().getBytes(Charset.defaultCharset());
			ObjectMetadata om = new ObjectMetadata();
			om.setContentType("text/plain");
			om.setContentLength(bytes.length);
			String logBucket = System.getenv("logBucket");
			s3.putObject(logBucket, "log-entry-" + UUID.randomUUID().toString(), new ByteArrayInputStream(bytes), om);
		}

	}

}
