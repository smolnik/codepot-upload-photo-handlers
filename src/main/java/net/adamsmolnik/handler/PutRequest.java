package net.adamsmolnik.handler;

import java.time.ZonedDateTime;
import java.util.Optional;

import net.adamsmolnik.handler.model.ImageMetadata;

/**
 * Just helper data class. All member fields MUST BE NOT NULL and all of them
 * MUST be filled out.
 * 
 * @author asmolnik
 *
 */
class PutRequest {

	String userId;

	String principalId;

	String photoKey;

	String thumbnailKey;

	String srcPhotoName;

	ZonedDateTime zdt;

	Optional<ImageMetadata> imageMetadata;

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

	PutRequest withSrcPhotoName(String srcPhotoName) {
		this.srcPhotoName = srcPhotoName;
		return this;
	}

	PutRequest withZonedDateTime(ZonedDateTime zdt) {
		this.zdt = zdt;
		return this;
	}

	PutRequest withImageMetadata(Optional<ImageMetadata> imageMetadata) {
		this.imageMetadata = imageMetadata;
		return this;
	}

}
