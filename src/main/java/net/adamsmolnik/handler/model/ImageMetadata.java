package net.adamsmolnik.handler.model;

import java.util.Date;

/**
 * @author asmolnik
 *
 */
public class ImageMetadata {

	private String madeBy, model;

	private Date photoTaken;

	public ImageMetadata withPhotoTaken(Date photoTaken) {
		this.photoTaken = photoTaken;
		return this;
	}

	public ImageMetadata withMadeBy(String madeBy) {
		this.madeBy = madeBy;
		return this;
	}

	public ImageMetadata withModel(String model) {
		this.model = model;
		return this;
	}

	public String getMadeBy() {
		return madeBy;
	}

	public String getModel() {
		return model;
	}

	public Date getPhotoTaken() {
		return photoTaken;
	}

}
