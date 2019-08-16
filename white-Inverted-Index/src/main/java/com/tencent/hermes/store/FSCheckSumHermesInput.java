package com.tencent.hermes.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.io.IOException;

public class FSCheckSumHermesInput extends InputStreamHermesInput{
	protected final Path directory;

	protected FSCheckSumHermesInput(String name, Path directory) throws IOException {
		super("FSCheckSumHermesInput(path=\"" + directory.resolve(name) + "\")", 
				Files.newInputStream(directory.resolve(name),StandardOpenOption.READ),
				FSCheckSumHermesOutput.CHUNK_SIZE);
		this.directory = directory.resolve(name);
	}

}
