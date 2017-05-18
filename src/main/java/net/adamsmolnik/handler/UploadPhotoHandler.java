package net.adamsmolnik.handler;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;
import net.adamsmolnik.handler.model.ImageMetadata;
import net.adamsmolnik.handler.util.ImageMetadataExplorer;
import net.adamsmolnik.handler.util.ImageResizer;
import net.adamsmolnik.handler.util.ResizerResult;

/**
 * A simplified, single-threaded variation of multithreaded UploadPhotoHandler
 * intended for workshop's purposes.
 * 
 * @author asmolnik
 *
 */
public class UploadPhotoHandler {

	private static final String STUDENT_PREFIX = "000";

	private static final String PHOTOS_TABLE_NAME = STUDENT_PREFIX + "-photos";

	private static final String DEST_BUCKET = STUDENT_PREFIX + "-photos";

	private static final String KEY_PREFIX = "photos/";

	private static final String JPEG_EXT = "jpg";

	private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-A");

	private static final int THUMBNAIL_SIZE = 300;

	private static final int WEB_IMAGE_SIZE = 1080;

	private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

	private final DynamoDB db = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());

	public void handle(S3Event s3Event, Context context) {
		LambdaLogger log = context.getLogger();
		s3Event.getRecords().forEach(record -> {
			try {
				process(new S3ObjectStream(record.getS3(), record.getUserIdentity()), log);
			} catch (IOException e) {
				throw new UploadPhotoHandlerException(e);
			}
		});
	}

	private void process(S3ObjectStream os, LambdaLogger log) throws IOException {
		String srcKey = os.getKey();
		log.log("File uploaded: " + srcKey);
		String userId = os.getUserId();
		String userKeyPrefix = KEY_PREFIX + userId + "/";
		Optional<ImageMetadata> imd = new ImageMetadataExplorer().explore(os.newCachedInputStream());
		ZonedDateTime zdt = imd.isPresent() && imd.get().getPhotoTaken().isPresent()
				? ZonedDateTime.ofInstant(Instant.ofEpochMilli(imd.get().getPhotoTaken().get().getTime()), ZoneId.of("UTC")) : ZonedDateTime.now();
		String baseDestKey = createDestKey(zdt, JPEG_EXT);
		String photoKey = userKeyPrefix + baseDestKey;
		String thumbnailKey = userKeyPrefix + "thumbnails/" + baseDestKey;
		putS3Object(photoKey, new ImageResizer(os.newCachedInputStream(), WEB_IMAGE_SIZE).resize(), s3);
		putS3Object(thumbnailKey, new ImageResizer(os.newCachedInputStream(), THUMBNAIL_SIZE).resize(), s3);

		Item item = new Item().withPrimaryKey("userId", userId, "photoKey", photoKey)
				.withString("photoTakenDate", zdt.format(DateTimeFormatter.ISO_LOCAL_DATE))
				.withString("photoTakenTime", zdt.format(DateTimeFormatter.ISO_LOCAL_TIME)).withString("thumbnailKey", thumbnailKey)
				.withString("bucket", DEST_BUCKET).with("principalId", os.getPrincipalId()).withString("srcPhotoName", srcKey);
		if (imd.isPresent()) {
			ImageMetadata imdObject = imd.get();
			addOptionalAtribute(item, "madeBy", imdObject.getMadeBy());
			addOptionalAtribute(item, "model", imdObject.getModel());
			if (!imdObject.getPhotoTaken().isPresent()) {
				item.withString("warning", "Missing photo taken date - date/time of the upload event has been used as a default");
			}
		} else {
			item.withString("warning", "Missing photo/image metadata to extract");
		}

		db.getTable(PHOTOS_TABLE_NAME).putItem(item);
	}

	private void putS3Object(String objectKey, ResizerResult rr, AmazonS3 s3) {
		ObjectMetadata md = new ObjectMetadata();
		md.setContentLength(rr.getSize());
		md.setContentType("image/jpeg");
		s3.putObject(DEST_BUCKET, objectKey, rr.getInputStream(), md);
	}

	private Item addOptionalAtribute(Item item, String attrName, Optional<String> attrValue) {
		if (!attrValue.isPresent()) {
			return item;
		}
		return item.withString(attrName, attrValue.get());
	}

	private String createDestKey(ZonedDateTime zdt, String ext) {
		return zdt.format(DT_FORMATTER) + "-" + Integer.toHexString(UUID.randomUUID().toString().hashCode()) + "." + ext;
	}

}
