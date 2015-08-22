package net.adamsmolnik.handler.model;

import java.util.Date;
import java.util.Optional;

/**
 * @author asmolnik
 *
 */
public class ImageMetadata {

	private Optional<String> madeBy, model;

	private Optional<Date> photoTaken;

	public ImageMetadata withPhotoTaken(Date photoTaken) {
		this.photoTaken = Optional.ofNullable(photoTaken);
		return this;
	}

	public ImageMetadata withMadeBy(String madeBy) {
		this.madeBy = Optional.ofNullable(madeBy);
		return this;
	}

	public ImageMetadata withModel(String model) {
		this.model = Optional.ofNullable(model);
		return this;
	}

	public Optional<String> getMadeBy() {
		return madeBy;
	}

	public Optional<String> getModel() {
		return model;
	}

	public Optional<Date> getPhotoTaken() {
		return photoTaken;
	}

}
