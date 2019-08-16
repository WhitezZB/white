package com.tencent.hermes.store;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class FSCheckSumHermesOutput extends OutputStreamHermesOutput {

	
    
    protected final Path directory;
        
    public FSCheckSumHermesOutput(String name,Path directory) throws IOException {
      this(directory,name, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);     
    }

    FSCheckSumHermesOutput(Path directory, String name, OpenOption... options) throws IOException {
      super("FSHermesOutput(path=\"" + directory.resolve(name) + "\")", name, new FilterOutputStream(Files.newOutputStream(directory.resolve(name), options)) {
        // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
          while (length > 0) {
            final int chunk = Math.min(length, CHUNK_SIZE);
            out.write(b, offset, chunk);
            length -= chunk;
            offset += chunk;
          }
        }
      }, CHUNK_SIZE);
      this.directory = directory;
    }
  }
