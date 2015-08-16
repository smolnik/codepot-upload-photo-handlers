package net.adamsmolnik.handler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;
import com.amazonaws.services.s3.event.S3EventNotification.UserIdentityEntity;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;

/**
 * @author asmolnik
 *
 */
public class S3ObjectStream {

	private final AmazonS3 s3 = new AmazonS3Client();

	private final String userId;

	private final String principalId;

	private final String bucket, key;

	private final long size;

	private volatile byte[] imgAsBytes;

	private final Object lock = new Object();

	public S3ObjectStream(S3Entity s3Entity, UserIdentityEntity userIdentityEntity) {
		this.bucket = s3Entity.getBucket().getName();
		S3ObjectEntity s3Object = s3Entity.getObject();
		this.key = s3Object.getKey();
		this.size = s3Object.getSizeAsLong();
		this.principalId = userIdentityEntity.getPrincipalId();
		this.userId = mapIdentity(userIdentityEntity);
	}

	private String mapIdentity(UserIdentityEntity userIdentityEntity) {
		// TODO
		return "default";
	}

	public String getBucket() {
		return bucket;
	}

	public String getKey() {
		return key;
	}

	public InputStream newInputStream() {
		return s3.getObject(bucket, key).getObjectContent();
	}

	public InputStream newCachedInputStream() {
		try {
			if (imgAsBytes == null) {
				synchronized (lock) {
					if (imgAsBytes == null) {
						imgAsBytes = new byte[(int) size];
						InputStream is = s3.getObject(bucket, key).getObjectContent();
						int pos = 0, length;
						while ((length = is.read(imgAsBytes, pos, imgAsBytes.length)) > 0) {
							pos += length;
						}
					}
				}
			}
		} catch (IOException e) {
			throw new UploadPhotoHandlerException(e);
		}
		return new ByteArrayInputStream(imgAsBytes);
	}

	public String getUserId() {
		return userId;
	}

	public long getSize() {
		return size;
	}

	public String getPrincipalId() {
		return principalId;
	}

}
