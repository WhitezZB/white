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
package com.tencent.hermes.store.bulk;


import java.io.IOException;
import java.util.Arrays;



/**
 * Simplistic compression for array of unsigned long values.
 * Each value is {@code >= 0} and {@code <=} a specified maximum value.  The
 * values are stored as packed ints, with each value
 * consuming a fixed number of bits.
 *
 * @lucene.internal
 */
public class PackedInts {

  /**
   * At most 700% memory overhead, always select a direct implementation.
   */
  public static final float FASTEST = 7f;

  /**
   * At most 50% memory overhead, always select a reasonably fast implementation.
   */
  public static final float FAST = 0.5f;

  /**
   * At most 25% memory overhead.
   */
  public static final float DEFAULT = 0.25f;

  /**
   * No memory overhead at all, but the returned implementation may be slow.
   */
  public static final float COMPACT = 0f;

  /**
   * Default amount of memory to use for bulk operations.
   */
  public static final int DEFAULT_BUFFER_SIZE = 1024; // 1K

  public final static String CODEC_NAME = "PackedInts";
  public static final int VERSION_MONOTONIC_WITHOUT_ZIGZAG = 2;
  public final static int VERSION_START = VERSION_MONOTONIC_WITHOUT_ZIGZAG;
  public final static int VERSION_CURRENT = VERSION_MONOTONIC_WITHOUT_ZIGZAG;

  /**
   * Check the validity of a version number.
   */
  public static void checkVersion(int version) {
    if (version < VERSION_START) {
      throw new IllegalArgumentException("Version is too old, should be at least " + VERSION_START + " (got " + version + ")");
    } else if (version > VERSION_CURRENT) {
      throw new IllegalArgumentException("Version is too new, should be at most " + VERSION_CURRENT + " (got " + version + ")");
    }
  }

  
  public static final int MAX_SUPPORTED_BITS_PER_VALUE = 32;
  private static final int[] SUPPORTED_BITS_PER_VALUE = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32};
  /**
   * A format to write packed ints.
   *
   * @lucene.internal
   */
  public enum Format {
    /**
     * Compact format, all bits are written contiguously.
     */
    PACKED(0) {

      @Override
      public long byteCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
        return (long) Math.ceil((double) valueCount * bitsPerValue / 8);      
      }

    },

    /**
     * A format that may insert padding bits to improve encoding and decoding
     * speed. Since this format doesn't support all possible bits per value, you
     * should never use it directly, but rather use
     * {@link PackedInts#fastestFormatAndBits(int, int, float)} to find the
     * format that best suits your needs.
     */
    PACKED_SINGLE_BLOCK(1) {

      @Override
      public int longCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
        final int valuesPerBlock = 64 / bitsPerValue;
        return (int) Math.ceil((double) valueCount / valuesPerBlock);
      }

      @Override
      public boolean isSupported(int bitsPerValue) {
        return Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) >= 0;
      }

      @Override
      public float overheadPerValue(int bitsPerValue) {
        assert isSupported(bitsPerValue);
        final int valuesPerBlock = 64 / bitsPerValue;
        final int overhead = 64 % bitsPerValue;
        return (float) overhead / valuesPerBlock;
      }

    };

    /**
     * Get a format according to its ID.
     */
    public static Format byId(int id) {
      for (Format format : Format.values()) {
        if (format.getId() == id) {
          return format;
        }
      }
      throw new IllegalArgumentException("Unknown format id: " + id);
    }

    private Format(int id) {
      this.id = id;
    }

    public int id;

    /**
     * Returns the ID of the format.
     */
    public int getId() {
      return id;
    }

    /**
     * Computes how many byte blocks are needed to store <code>values</code>
     * values of size <code>bitsPerValue</code>.
     */
    public long byteCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
      assert bitsPerValue >= 0 && bitsPerValue <= 64 : bitsPerValue;
      // assume long-aligned
      return 8L * longCount(packedIntsVersion, valueCount, bitsPerValue);
    }

    /**
     * Computes how many long blocks are needed to store <code>values</code>
     * values of size <code>bitsPerValue</code>.
     */
    public int longCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
      assert bitsPerValue >= 0 && bitsPerValue <= 64 : bitsPerValue;
      final long byteCount = byteCount(packedIntsVersion, valueCount, bitsPerValue);
      assert byteCount < 8L * Integer.MAX_VALUE;
      if ((byteCount % 8) == 0) {
        return (int) (byteCount / 8);
      } else {
        return (int) (byteCount / 8 + 1);
      }
    }

    /**
     * Tests whether the provided number of bits per value is supported by the
     * format.
     */
    public boolean isSupported(int bitsPerValue) {
      return bitsPerValue >= 1 && bitsPerValue <= 64;
    }

    /**
     * Returns the overhead per value, in bits.
     */
    public float overheadPerValue(int bitsPerValue) {
      assert isSupported(bitsPerValue);
      return 0f;
    }

    /**
     * Returns the overhead ratio (<code>overhead per value / bits per value</code>).
     */
    public final float overheadRatio(int bitsPerValue) {
      assert isSupported(bitsPerValue);
      return overheadPerValue(bitsPerValue) / bitsPerValue;
    }
  }

  /**
   * Simple class that holds a format and a number of bits per value.
   */
  public static class FormatAndBits {
    public final Format format;
    public final int bitsPerValue;
    public FormatAndBits(Format format, int bitsPerValue) {
      this.format = format;
      this.bitsPerValue = bitsPerValue;
    }

    @Override
    public String toString() {
      return "FormatAndBits(format=" + format + " bitsPerValue=" + bitsPerValue + ")";
    }
  }

  /**
   * Try to find the {@link Format} and number of bits per value that would
   * restore from disk the fastest reader whose overhead is less than
   * <code>acceptableOverheadRatio</code>.
   * <p>
   * The <code>acceptableOverheadRatio</code> parameter makes sense for
   * random-access {@link Reader}s. In case you only plan to perform
   * sequential access on this stream later on, you should probably use
   * {@link PackedInts#COMPACT}.
   * <p>
   * If you don't know how many values you are going to write, use
   * <code>valueCount = -1</code>.
   */
  public static final int MAX_SIZE = Integer.MAX_VALUE / 3;
  public static FormatAndBits fastestFormatAndBits(int valueCount, int bitsPerValue, float acceptableOverheadRatio) {
    if (valueCount == -1) {
      valueCount = Integer.MAX_VALUE;
    }

    acceptableOverheadRatio = Math.max(COMPACT, acceptableOverheadRatio);
    acceptableOverheadRatio = Math.min(FASTEST, acceptableOverheadRatio);
    float acceptableOverheadPerValue = acceptableOverheadRatio * bitsPerValue; // in bits

    int maxBitsPerValue = bitsPerValue + (int) acceptableOverheadPerValue;

    int actualBitsPerValue = -1;
    Format format = Format.PACKED;

    if (bitsPerValue <= 8 && maxBitsPerValue >= 8) {
      actualBitsPerValue = 8;
    } else if (bitsPerValue <= 16 && maxBitsPerValue >= 16) {
      actualBitsPerValue = 16;
    } else if (bitsPerValue <= 32 && maxBitsPerValue >= 32) {
      actualBitsPerValue = 32;
    } else if (bitsPerValue <= 64 && maxBitsPerValue >= 64) {
      actualBitsPerValue = 64;
    } else if (valueCount <= MAX_SIZE && bitsPerValue <= 24 && maxBitsPerValue >= 24) {
      actualBitsPerValue = 24;
    } else if (valueCount <= MAX_SIZE && bitsPerValue <= 48 && maxBitsPerValue >= 48) {
      actualBitsPerValue = 48;
    } else {
      for (int bpv = bitsPerValue; bpv <= maxBitsPerValue; ++bpv) {
        if (Format.PACKED_SINGLE_BLOCK.isSupported(bpv)) {
          float overhead = Format.PACKED_SINGLE_BLOCK.overheadPerValue(bpv);
          float acceptableOverhead = acceptableOverheadPerValue + bitsPerValue - bpv;
          if (overhead <= acceptableOverhead) {
            actualBitsPerValue = bpv;
            format = Format.PACKED_SINGLE_BLOCK;
            break;
          }
        }
      }
      if (actualBitsPerValue < 0) {
        actualBitsPerValue = bitsPerValue;
      }
    }

    return new FormatAndBits(format, actualBitsPerValue);
  }

  /**
   * A decoder for packed integers.
   */
  public static interface Decoder {

    /**
     * The minimum number of long blocks to encode in a single iteration, when
     * using long encoding.
     */
    int longBlockCount();

    /**
     * The number of values that can be stored in {@link #longBlockCount()} long
     * blocks.
     */
    int longValueCount();

    /**
     * The minimum number of byte blocks to encode in a single iteration, when
     * using byte encoding.
     */
    int byteBlockCount();

    /**
     * The number of values that can be stored in {@link #byteBlockCount()} byte
     * blocks.
     */
    int byteValueCount();

    /**
     * Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations);

    /**
     * Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations);

    /**
     * Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations);

    /**
     * Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations);

  }

  /**
   * An encoder for packed integers.
   */
  public static interface Encoder {

    /**
     * The minimum number of long blocks to encode in a single iteration, when
     * using long encoding.
     */
    int longBlockCount();

    /**
     * The number of values that can be stored in {@link #longBlockCount()} long
     * blocks.
     */
    int longValueCount();

    /**
     * The minimum number of byte blocks to encode in a single iteration, when
     * using byte encoding.
     */
    int byteBlockCount();

    /**
     * The number of values that can be stored in {@link #byteBlockCount()} byte
     * blocks.
     */
    int byteValueCount();

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>8 * iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(long[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>8 * iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(int[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations);

  }












  /**
   * Get a {@link Decoder}.
   *
   * @param format         the format used to store packed ints
   * @param version        the compatibility version
   * @param bitsPerValue   the number of bits per value
   * @return a decoder
   */
  public static Decoder getDecoder(Format format, int version, int bitsPerValue) {
    checkVersion(version);
    return BulkOperation.of(format, bitsPerValue);
  }

  /**
   * Get an {@link Encoder}.
   *
   * @param format         the format used to store packed ints
   * @param version        the compatibility version
   * @param bitsPerValue   the number of bits per value
   * @return an encoder
   */
  public static Encoder getEncoder(Format format, int version, int bitsPerValue) {
    checkVersion(version);
    return BulkOperation.of(format, bitsPerValue);
  }




  /** Returns how many bits are required to hold values up
   *  to and including maxValue
   *  NOTE: This method returns at least 1.
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @lucene.internal
   */
  public static int bitsRequired(long maxValue) {
    if (maxValue < 0) {
      throw new IllegalArgumentException("maxValue must be non-negative (got: " + maxValue + ")");
    }
    return unsignedBitsRequired(maxValue);
  }

  /** Returns how many bits are required to store <code>bits</code>,
   * interpreted as an unsigned value.
   * NOTE: This method returns at least 1.
   * @lucene.internal
   */
  public static int unsignedBitsRequired(long bits) {
    return Math.max(1, 64 - Long.numberOfLeadingZeros(bits));
  }

  /**
   * Calculates the maximum unsigned long that can be expressed with the given
   * number of bits.
   * @param bitsPerValue the number of bits available for any given value.
   * @return the maximum value for the given bits.
   * @lucene.internal
   */
  public static long maxValue(int bitsPerValue) {
    return bitsPerValue == 64 ? Long.MAX_VALUE : ~(~0L << bitsPerValue);
  }
  

}
