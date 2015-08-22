package net.adamsmolnik.handler.util;

import java.io.InputStream;
import java.util.Optional;

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

	public Optional<ImageMetadata> explore(InputStream is) {
		try {
			Metadata metadata = ImageMetadataReader.readMetadata(is);
			Directory d = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
			if (d == null) {
				return Optional.empty();
			}

			ImageMetadata imd = new ImageMetadata();
			imd.withMadeBy(d.getString(ExifDirectoryBase.TAG_MAKE));
			imd.withModel(d.getString(ExifDirectoryBase.TAG_MODEL));
			imd.withPhotoTaken(d.getDate(ExifDirectoryBase.TAG_DATETIME));
			return Optional.of(imd);
		} catch (Exception e) {
			throw new UploadPhotoHandlerException(e);
		}
	}

}
