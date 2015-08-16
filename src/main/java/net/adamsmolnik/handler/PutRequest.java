package net.adamsmolnik.handler;

import java.time.ZonedDateTime;

import net.adamsmolnik.handler.model.ImageMetadata;

/**
 * Just helper data class.
 * 
 * @author asmolnik
 *
 */
class PutRequest {

	String userId;

	String principalId;

	String photoKey;

	String thumbnailKey;

	ZonedDateTime zdt;

	ImageMetadata imd;

	PutRequest withUserId(String userId) {
		this.userId = userId;
		return this;
	}

	PutRequest withPrincipalId(String principalId) {
		this.principalId = principalId;
		return this;
	}

	PutRequest withPhotoKey(String photoKey) {
		this.photoKey = photoKey;
		return this;
	}

	PutRequest withThumbnailKey(String thumbnailKey) {
		this.thumbnailKey = thumbnailKey;
		return this;
	}

	PutRequest withZonedDateTime(ZonedDateTime zdt) {
		this.zdt = zdt;
		return this;
	}

	PutRequest withImageMetadata(ImageMetadata imd) {
		this.imd = imd;
		return this;
	}

}
