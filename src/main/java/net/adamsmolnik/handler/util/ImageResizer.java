package net.adamsmolnik.handler.util;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;

/**
 * @author asmolnik
 *
 */
public class ImageResizer {

	private final BufferedImage orgImg;

	private final int targetWidth;

	private final double ratio;

	public ImageResizer(InputStream is, int targetWidth) {
		try {
			this.targetWidth = targetWidth;
			this.orgImg = ImageIO.read(is);
		} catch (IOException e) {
			throw new UploadPhotoHandlerException(e);
		}
		ratio = ((double) orgImg.getHeight()) / orgImg.getWidth();
	}

	public ResizerResult resize() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			ImageIO.write(doResize(orgImg, targetWidth, ratio), "jpg", baos);
			byte[] bytes = baos.toByteArray();
			return new ResizerResult(bytes.length, new ByteArrayInputStream(bytes));
		} catch (IOException e) {
			throw new UploadPhotoHandlerException(e);
		}
	}

	private static BufferedImage doResize(BufferedImage orgImg, int targetWidth, double ratio) {
		return toBufferedImage(orgImg.getScaledInstance(targetWidth, (int) (targetWidth * ratio), Image.SCALE_SMOOTH),
				Transparency.OPAQUE == orgImg.getTransparency() ? BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB);
	}

	public static BufferedImage toBufferedImage(Image img, int type) {
		BufferedImage bi = new BufferedImage(img.getWidth(null), img.getHeight(null), type);
		Graphics2D gr = bi.createGraphics();
		gr.drawImage(img, 0, 0, null);
		gr.dispose();
		return bi;
	}

}
