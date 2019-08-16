package org.white.parquet;

import java.io.IOException;

public interface PReader<T> {

	public T read() throws Exception;
}
