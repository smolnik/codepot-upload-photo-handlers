package net.adamsmolnik.util;

import java.io.InputStream;

public class ResizerResult {

	private final int size;

	private final InputStream inputStream;

	public ResizerResult(int size, InputStream inputStream) {
		this.size = size;
		this.inputStream = inputStream;
	}

	public int getSize() {
		return size;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

}
