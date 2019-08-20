/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.hermes.store.utils;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.tencent.hermes.store.DataInput;
import com.tencent.hermes.store.HermesInput;
import com.tencent.hermes.store.HermesOutput;



/**
 * Utility class for reading and writing versioned headers.
 * <p>
 * Writing codec headers is useful to ensure that a file is in 
 * the format you think it is.
 * 
 */

public final class CodecUtil {
  private CodecUtil() {} // no instance
  
  public final static String name = "White";

  /**
   * Constant to identify the start of a codec header.
   */
  public final static int CODEC_MAGIC = 0x3fd76c17;
  /**
   * Constant to identify the start of a codec footer.
   */
  public final static int FOOTER_MAGIC = ~CODEC_MAGIC;

  /**
   * Writes a codec header, which records both a string to
   * identify the file and a version number. This header can
   * be parsed and validated with 
   * {@link #checkHeader(DataInput, String, int, int) checkHeader()}.
   * <p>
   * CodecHeader --&gt; Magic,CodecName,Version
   * <ul>
   *    <li>Magic --&gt; {@link DataOutput#writeInt Uint32}. This
   *        identifies the start of the header. It is always {@value #CODEC_MAGIC}.
   *    <li>CodecName --&gt; {@link DataOutput#writeString String}. This
   *        is a string to identify this file.
   *    <li>Version --&gt; {@link DataOutput#writeInt Uint32}. Records
   *        the version of the file.
   * </ul>
   * <p>
   * Note that the length of a codec header depends only upon the
   * name of the codec, so this length can be computed at any time
   * with {@link #headerLength(String)}.
   * 
   * @param out Output stream
   * @param codec String to identify this file. It should be simple ASCII, 
   *              less than 128 characters in length.
   * @param version Version number
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @throws IllegalArgumentException If the codec name is not simple ASCII, or is more than 127 characters in length
   */
  public static void writeHeader(HermesOutput out, String codec, int version) throws IOException {
    BytesRef bytes = new BytesRef(codec);
    if (bytes.length != codec.length() || bytes.length >= 128) {
      throw new IllegalArgumentException("codec must be simple ASCII, less than 128 characters in length [got " + codec + "]");
    }
    out.writeInt(CODEC_MAGIC);
    out.writeString(codec);
    out.writeInt(version);
  }
  
  /**
   * Writes a codec header for an index file, which records both a string to
   * identify the format of the file, a version number, and data to identify
   * the file instance (ID and auxiliary suffix such as generation).
   * <p>
   * This header can be parsed and validated with 
   * {@link #checkIndexHeader(DataInput, String, int, int, byte[], String) checkIndexHeader()}.
   * <p>
   * IndexHeader --&gt; CodecHeader,ObjectID,ObjectSuffix
   * <ul>
   *    <li>CodecHeader   --&gt; {@link #writeHeader}
   *    <li>ObjectID     --&gt; {@link DataOutput#writeByte byte}<sup>16</sup>
   *    <li>ObjectSuffix --&gt; SuffixLength,SuffixBytes
   *    <li>SuffixLength  --&gt; {@link DataOutput#writeByte byte}
   *    <li>SuffixBytes   --&gt; {@link DataOutput#writeByte byte}<sup>SuffixLength</sup>
   * </ul>
   * <p>
   * Note that the length of an index header depends only upon the
   * name of the codec and suffix, so this length can be computed at any time
   * with {@link #indexHeaderLength(String,String)}.
   * 
   * @param out Output stream
   * @param codec String to identify the format of this file. It should be simple ASCII, 
   *              less than 128 characters in length.
   * @param id Unique identifier for this particular file instance.
   * @param suffix auxiliary suffix information for the file. It should be simple ASCII,
   *              less than 256 characters in length.
   * @param version Version number
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @throws IllegalArgumentException If the codec name is not simple ASCII, or 
   *         is more than 127 characters in length, or if id is invalid,
   *         or if the suffix is not simple ASCII, or more than 255 characters
   *         in length.
   */
  public static void writeIndexHeader(HermesOutput out, String codec, int version, byte[] id, String suffix) throws IOException {
    if (id.length != StringHelper.ID_LENGTH) {
      throw new IllegalArgumentException("Invalid id: " + StringHelper.idToString(id));
    }
    writeHeader(out, codec, version);
    out.writeBytes(id, 0, id.length);
    BytesRef suffixBytes = new BytesRef(suffix);
    if (suffixBytes.length != suffix.length() || suffixBytes.length >= 256) {
      throw new IllegalArgumentException("suffix must be simple ASCII, less than 256 characters in length [got " + suffix + "]");
    }
    out.writeByte((byte) suffixBytes.length);
    out.writeBytes(suffixBytes.bytes, suffixBytes.offset, suffixBytes.length);
  }

  /**
   * Computes the length of a codec header.
   * 
   * @param codec Codec name.
   * @return length of the entire codec header.
   * @see #writeHeader(DataOutput, String, int)
   */
  public static int headerLength(String codec) {
    return 9+codec.length();
  }
  
  /**
   * Computes the length of an index header.
   * 
   * @param codec Codec name.
   * @return length of the entire index header.
   * @see #writeIndexHeader(DataOutput, String, int, byte[], String)
   */
  public static int indexHeaderLength(String codec, String suffix) {
    return headerLength(codec) + StringHelper.ID_LENGTH + 1 + suffix.length();
  }

  /**
   * Reads and validates a header previously written with 
   * {@link #writeHeader(DataOutput, String, int)}.
   * <p>
   * When reading a file, supply the expected <code>codec</code> and
   * an expected version range (<code>minVersion to maxVersion</code>).
   * 
   * @param in Input stream, positioned at the point where the
   *        header was previously written. Typically this is located
   *        at the beginning of the file.
   * @param codec The expected codec name.
   * @param minVersion The minimum supported expected version number.
   * @param maxVersion The maximum supported expected version number.
   * @return The actual version found, when a valid header is found 
   *         that matches <code>codec</code>, with an actual version 
   *         where {@code minVersion <= actual <= maxVersion}.
   *         Otherwise an exception is thrown.
   * @throws CorruptIndexException If the first four bytes are not
   *         {@link #CODEC_MAGIC}, or if the actual codec found is
   *         not <code>codec</code>.
   * @throws IndexFormatTooOldException If the actual version is less 
   *         than <code>minVersion</code>.
   * @throws IndexFormatTooNewException If the actual version is greater 
   *         than <code>maxVersion</code>.
   * @throws IOException If there is an I/O error reading from the underlying medium.
   * @see #writeHeader(DataOutput, String, int)
   */
  public static int checkHeader(DataInput in, String codec, int minVersion, int maxVersion) throws IOException {
    // Safety to guard against reading a bogus string:
    final int actualHeader = in.readInt();
    if (actualHeader != CODEC_MAGIC) {
      throw new CorruptException("codec header mismatch: actual header=" + actualHeader + " vs expected header=" + CODEC_MAGIC, in);
    }
    return checkHeaderNoMagic(in, codec, minVersion, maxVersion);
  }

  /** Like {@link
   *  #checkHeader(DataInput,String,int,int)} except this
   *  version assumes the first int has already been read
   *  and validated from the input. */
  public static int checkHeaderNoMagic(DataInput in, String codec, int minVersion, int maxVersion) throws IOException {
    final String actualCodec = in.readString();
    if (!actualCodec.equals(codec)) {
      throw new CorruptException("codec mismatch: actual codec=" + actualCodec + " vs expected codec=" + codec, in);
    }

    final int actualVersion = in.readInt();
    if (actualVersion < minVersion) {
      throw new FormatTooOldException(in, actualVersion, minVersion, maxVersion);
    }
    if (actualVersion > maxVersion) {
      throw new FormatTooNewException(in, actualVersion, minVersion, maxVersion);
    }

    return actualVersion;
  }
  
  /**
   * Reads and validates a header previously written with 
   * {@link #writeIndexHeader(DataOutput, String, int, byte[], String)}.
   * <p>
   * When reading a file, supply the expected <code>codec</code>,
   * expected version range (<code>minVersion to maxVersion</code>),
   * and object ID and suffix.
   * 
   * @param in Input stream, positioned at the point where the
   *        header was previously written. Typically this is located
   *        at the beginning of the file.
   * @param codec The expected codec name.
   * @param minVersion The minimum supported expected version number.
   * @param maxVersion The maximum supported expected version number.
   * @param expectedID The expected object identifier for this file.
   * @param expectedSuffix The expected auxiliary suffix for this file.
   * @return The actual version found, when a valid header is found 
   *         that matches <code>codec</code>, with an actual version 
   *         where {@code minVersion <= actual <= maxVersion}, 
   *         and matching <code>expectedID</code> and <code>expectedSuffix</code>
   *         Otherwise an exception is thrown.
   * @throws CorruptIndexException If the first four bytes are not
   *         {@link #CODEC_MAGIC}, or if the actual codec found is
   *         not <code>codec</code>, or if the <code>expectedID</code>
   *         or <code>expectedSuffix</code> do not match.
   * @throws IndexFormatTooOldException If the actual version is less 
   *         than <code>minVersion</code>.
   * @throws IndexFormatTooNewException If the actual version is greater 
   *         than <code>maxVersion</code>.
   * @throws IOException If there is an I/O error reading from the underlying medium.
   * @see #writeIndexHeader(DataOutput, String, int, byte[],String)
   */
  public static int checkIndexHeader(HermesInput in, String codec, int minVersion, int maxVersion, byte[] expectedID, String expectedSuffix) throws IOException {
    int version = checkHeader(in, codec, minVersion, maxVersion);
    checkIndexHeaderID(in, expectedID);
    checkIndexHeaderSuffix(in, expectedSuffix);
    return version;
  }

  /**
   * Expert: verifies the incoming {@link IndexInput} has an index header
   * and that its segment ID matches the expected one, and then copies
   * that index header into the provided {@link DataOutput}.  This is
   * useful when building compound files.
   *
   * @param in Input stream, positioned at the point where the
   *        index header was previously written. Typically this is located
   *        at the beginning of the file.
   * @param out Output stream, where the header will be copied to.
   * @param expectedID Expected segment ID
   * @throws CorruptIndexException If the first four bytes are not
   *         {@link #CODEC_MAGIC}, or if the <code>expectedID</code>
   *         does not match.
   * @throws IOException If there is an I/O error reading from the underlying medium.
   *
   * @lucene.internal 
   */
  public static void verifyAndCopyIndexHeader(HermesInput in, HermesOutput out, byte[] expectedID) throws IOException {
    // make sure it's large enough to have a header and footer
    if (in.length() < footerLength() + headerLength("")) {
      throw new CorruptException("compound sub-files must have a valid codec header and footer: file is too small (" + in.length() + " bytes)", in);
    }

    int actualHeader = in.readInt();
    if (actualHeader != CODEC_MAGIC) {
      throw new CorruptException("compound sub-files must have a valid codec header and footer: codec header mismatch: actual header=" + actualHeader + " vs expected header=" + CodecUtil.CODEC_MAGIC, in);
    }

    // we can't verify these, so we pass-through:
    String codec = in.readString();
    int version = in.readInt();

    // verify id:
    checkIndexHeaderID(in, expectedID);

    // we can't verify extension either, so we pass-through:
    int suffixLength = in.readByte() & 0xFF;
    byte[] suffixBytes = new byte[suffixLength];
    in.readBytes(suffixBytes, 0, suffixLength);

    // now write the header we just verified
    out.writeInt(CodecUtil.CODEC_MAGIC);
    out.writeString(codec);
    out.writeInt(version);
    out.writeBytes(expectedID, 0, expectedID.length);
    out.writeByte((byte) suffixLength);
    out.writeBytes(suffixBytes, 0, suffixLength);
  }


  /** Retrieves the full index header from the provided {@link IndexInput}.
   *  This throws {@link CorruptIndexException} if this file does
   * not appear to be an index file. */
  public static byte[] readIndexHeader(HermesInput in) throws IOException {
    in.seek(0);
    final int actualHeader = in.readInt();
    if (actualHeader != CODEC_MAGIC) {
      throw new CorruptException("codec header mismatch: actual header=" + actualHeader + " vs expected header=" + CODEC_MAGIC, in);
    }
    String codec = in.readString();
    in.readInt();
    in.seek(in.getFilePointer() + StringHelper.ID_LENGTH);
    int suffixLength = in.readByte() & 0xFF;
    byte[] bytes = new byte[headerLength(codec) + StringHelper.ID_LENGTH + 1 + suffixLength];
    in.seek(0);
    in.readBytes(bytes, 0, bytes.length);
    return bytes;
  }

  /** Retrieves the full footer from the provided {@link IndexInput}.  This throws
   *  {@link CorruptIndexException} if this file does not have a valid footer. */
  public static byte[] readFooter(HermesInput in) throws IOException {
    if (in.length() < footerLength()) {
      throw new CorruptException("misplaced codec footer (file truncated?): length=" + in.length() + " but footerLength==" + footerLength(), in);
    }
    in.seek(in.length() - footerLength());
    validateFooter(in);
    in.seek(in.length() - footerLength());
    byte[] bytes = new byte[footerLength()];
    in.readBytes(bytes, 0, bytes.length);
    return bytes;
  }
  
  /** Expert: just reads and verifies the object ID of an index header */
  public static byte[] checkIndexHeaderID(HermesInput in, byte[] expectedID) throws IOException {
    byte id[] = new byte[StringHelper.ID_LENGTH];
    in.readBytes(id, 0, id.length);
    if (!Arrays.equals(id, expectedID)) {
      throw new CorruptException("file mismatch, expected id=" + StringHelper.idToString(expectedID) 
                                                         + ", got=" + StringHelper.idToString(id), in);
    }
    return id;
  }
  
  /** Expert: just reads and verifies the suffix of an index header */
  public static String checkIndexHeaderSuffix(HermesInput in, String expectedSuffix) throws IOException {
    int suffixLength = in.readByte() & 0xFF;
    byte suffixBytes[] = new byte[suffixLength];
    in.readBytes(suffixBytes, 0, suffixBytes.length);
    String suffix = new String(suffixBytes, 0, suffixBytes.length, StandardCharsets.UTF_8);
    if (!suffix.equals(expectedSuffix)) {
      throw new CorruptException("file mismatch, expected suffix=" + expectedSuffix
                                                             + ", got=" + suffix, in);
    }
    return suffix;
  }
  
  /**
   * Writes a codec footer, which records both a checksum
   * algorithm ID and a checksum. This footer can
   * be parsed and validated with 
   * {@link #checkFooter(ChecksumIndexInput) checkFooter()}.
   * <p>
   * CodecFooter --&gt; Magic,AlgorithmID,Checksum
   * <ul>
   *    <li>Magic --&gt; {@link DataOutput#writeInt Uint32}. This
   *        identifies the start of the footer. It is always {@value #FOOTER_MAGIC}.
   *    <li>AlgorithmID --&gt; {@link DataOutput#writeInt Uint32}. This
   *        indicates the checksum algorithm used. Currently this is always 0,
   *        for zlib-crc32.
   *    <li>Checksum --&gt; {@link DataOutput#writeLong Uint64}. The
   *        actual checksum value for all previous bytes in the stream, including
   *        the bytes from Magic and AlgorithmID.
   * </ul>
   * 
   * @param out Output stream
   * @throws IOException If there is an I/O error writing to the underlying medium.
   */
  public static void writeFooter(HermesOutput out) throws IOException {
    out.writeInt(FOOTER_MAGIC);
    out.writeInt(0);
    writeCRC(out);
  }
  
  /**
   * Computes the length of a codec footer.
   * 
   * @return length of the entire codec footer.
   * @see #writeFooter(IndexOutput)
   */
  public static int footerLength() {
    return 16;
  }
  


  
  /** 
   * Returns (but does not validate) the checksum previously written by {@link #checkFooter}.
   * @return actual checksum value
   * @throws IOException if the footer is invalid
   */
  public static long retrieveChecksum(HermesInput in) throws IOException {
    if (in.length() < footerLength()) {
      throw new CorruptException("misplaced codec footer (file truncated?): length=" + in.length() + " but footerLength==" + footerLength(), in);
    }
    in.seek(in.length() - footerLength());
    validateFooter(in);
    return readCRC(in);
  }
  
  private static void validateFooter(HermesInput in) throws IOException {
    long remaining = in.length() - in.getFilePointer();
    long expected = footerLength();
    if (remaining < expected) {
      throw new CorruptException("misplaced codec footer (file truncated?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
    } else if (remaining > expected) {
      throw new CorruptException("misplaced codec footer (file extended?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
    }
    
    final int magic = in.readInt();
    if (magic != FOOTER_MAGIC) {
      throw new CorruptException("codec footer mismatch (file truncated?): actual footer=" + magic + " vs expected footer=" + FOOTER_MAGIC, in);
    }
    
    final int algorithmID = in.readInt();
    if (algorithmID != 0) {
      throw new CorruptException("codec footer mismatch: unknown algorithmID: " + algorithmID, in);
    }
  }
  

  
  /**
   * Reads CRC32 value as a 64-bit long from the input.
   * @throws CorruptIndexException if CRC is formatted incorrectly (wrong bits set)
   * @throws IOException if an i/o error occurs
   */
  static long readCRC(HermesInput input) throws IOException {
    long value = input.readLong();
    if ((value & 0xFFFFFFFF00000000L) != 0) {
      throw new CorruptException("Illegal CRC-32 checksum: " + value, input);
    }
    return value;
  }
  
  /**
   * Writes CRC32 value as a 64-bit long to the output.
   * @throws IllegalStateException if CRC is formatted incorrectly (wrong bits set)
   * @throws IOException if an i/o error occurs
   */
  static void writeCRC(HermesOutput output) throws IOException {
    long value = output.getChecksum();
    if ((value & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalStateException("Illegal CRC-32 checksum: " + value + " (resource=" + output + ")");
    }
    output.writeLong(value);
  }
}
