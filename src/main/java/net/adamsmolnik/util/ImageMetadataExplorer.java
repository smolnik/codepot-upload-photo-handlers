package net.adamsmolnik.util;

import java.io.InputStream;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifDirectoryBase;
import com.drew.metadata.exif.ExifIFD0Directory;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;
import net.adamsmolnik.handler.model.ImageMetadata;

/**
 * @author asmolnik
 *
 */
public class ImageMetadataExplorer {

	public ImageMetadata explore(InputStream is) {
		try {
			ImageMetadata imd = new ImageMetadata();
			Metadata metadata = ImageMetadataReader.readMetadata(is);
			Directory d = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
			imd.withMadeBy(d.getString(ExifDirectoryBase.TAG_MAKE));
			imd.withModel(d.getString(ExifDirectoryBase.TAG_MODEL));
			imd.withPhotoTaken(d.getDate(ExifDirectoryBase.TAG_DATETIME));
			return imd;
		} catch (Exception e) {
			throw new UploadPhotoHandlerException(e);
		}
	}

}
