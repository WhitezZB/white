package com.tencent.hermes.store;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/** Implementation class for buffered {@link IndexOutput} that writes to an {@link OutputStream}. */
public class OutputStreamHermesOutput extends HermesOutput {

  private final CRC32 crc = new CRC32();
  private final BufferedOutputStream os;
  
  private long bytesWritten = 0L;
  private boolean flushedOnClose = false;

  /**
   * Creates a new {@link OutputStreamHermesOutput} with the given buffer size. 
   * @param bufferSize the buffer size in bytes used to buffer writes internally.
   * @throws IllegalArgumentException if the given buffer size is less or equal to <tt>0</tt>
   */
  public OutputStreamHermesOutput(String resourceDescription, String name, OutputStream out, int bufferSize) {
    super(resourceDescription, name);
    this.os = new BufferedOutputStream(new CheckedOutputStream(out, crc), bufferSize);
  }

  @Override
  public final void writeByte(byte b) throws IOException {
    os.write(b);
    bytesWritten++;
  }
  
  @Override
  public final void writeBytes(byte[] b, int offset, int length) throws IOException {
    os.write(b, offset, length);
    bytesWritten += length;
  }

  @Override
  public void close() throws IOException {
    try (final OutputStream o = os) {
      // We want to make sure that os.flush() was running before close:
      // BufferedOutputStream may ignore IOExceptions while flushing on close().
      // We keep this also in Java 8, although it claims to be fixed there,
      // because there are more bugs around this! See:
      // # https://bugs.openjdk.java.net/browse/JDK-7015589
      // # https://bugs.openjdk.java.net/browse/JDK-8054565
      if (!flushedOnClose) {
        flushedOnClose = true; // set this BEFORE calling flush!
        o.flush();
      }
    }
  }
  
  @Override
  public final long getFilePointer() {
    return bytesWritten;
  }

  public final long getChecksum() throws IOException {
    os.flush();
    return crc.getValue();
  }
}
