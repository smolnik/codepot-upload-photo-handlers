package net.adamsmolnik.handler.exception;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author asmolnik
 *
 */
public class UploadPhotoHandlerException extends RuntimeException {

	private static final long serialVersionUID = 6833924346684528822L;

	public UploadPhotoHandlerException(Exception e) {
		super(e);
	}

	public UploadPhotoHandlerException(Collection<Exception> exs) {
		super(String.join(",", exs.stream().map(Exception::getLocalizedMessage).collect(Collectors.toList())));
	}

}
