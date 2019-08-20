package com.tencent.hermes.store.utils;

public final class FutureArrays {
  
  private FutureArrays() {} // no instance
  
  // methods in Arrays are defined stupid: they cannot use Objects.checkFromToIndex
  // they throw IAE (vs IOOBE) in the case of fromIndex > toIndex.
  // so this method works just like checkFromToIndex, but with that stupidity added.
  private static void checkFromToIndex(int fromIndex, int toIndex, int length) {
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex " + fromIndex + " > toIndex " + toIndex);
    }
    if (fromIndex < 0 || toIndex > length) {
      throw new IndexOutOfBoundsException("Range [" + fromIndex + ", " + toIndex + ") out-of-bounds for length " + length);
    }
  }

  // byte[]

  /**
   * Behaves like Java 9's Arrays.mismatch
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#mismatch-byte:A-int-int-byte:A-int-int-">Arrays.mismatch</a>
   */
  public static int mismatch(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++)
      if (a[i+aFromIndex] != b[i+bFromIndex])
        return i;
    return aLen == bLen ? -1 : len;
  }
  
  /**
   * Behaves like Java 9's Arrays.compareUnsigned
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compareUnsigned-byte:A-int-int-byte:A-int-int-">Arrays.compareUnsigned</a>
   */
  public static int compareUnsigned(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++) {
      int aByte = a[i+aFromIndex] & 0xFF;
      int bByte = b[i+bFromIndex] & 0xFF;
      int diff = aByte - bByte;
      if (diff != 0) {
        return diff;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
  }
  
  /**
   * Behaves like Java 9's Arrays.equals
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-byte:A-int-int-byte:A-int-int-">Arrays.equals</a>
   */
  public static boolean equals(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
      return false;
    }
    for (int i = 0; i < aLen; i++) {
      if (a[i+aFromIndex] != b[i+bFromIndex]) {
        return false;
      }
    }
    return true;
  }

  // char[]

  /**
   * Behaves like Java 9's Arrays.mismatch
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#mismatch-char:A-int-int-char:A-int-int-">Arrays.mismatch</a>
   */
  public static int mismatch(char[] a, int aFromIndex, int aToIndex, char[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++)
      if (a[i+aFromIndex] != b[i+bFromIndex])
        return i;
    return aLen == bLen ? -1 : len;
  }
  
  /**
   * Behaves like Java 9's Arrays.compare
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-char:A-int-int-char:A-int-int-">Arrays.compare</a>
   */
  public static int compare(char[] a, int aFromIndex, int aToIndex, char[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++) {
      int aInt = a[i+aFromIndex];
      int bInt = b[i+bFromIndex];
      if (aInt > bInt) {
        return 1;
      } else if (aInt < bInt) {
        return -1;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
  }
  
  /**
   * Behaves like Java 9's Arrays.equals
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-char:A-int-int-char:A-int-int-">Arrays.equals</a>
   */
  public static boolean equals(char[] a, int aFromIndex, int aToIndex, char[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
      return false;
    }
    for (int i = 0; i < aLen; i++) {
      if (a[i+aFromIndex] != b[i+bFromIndex]) {
        return false;
      }
    }
    return true;
  }

  // int[]
  
  /**
   * Behaves like Java 9's Arrays.compare
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-int:A-int-int-int:A-int-int-">Arrays.compare</a>
   */
  public static int compare(int[] a, int aFromIndex, int aToIndex, int[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++) {
      int aInt = a[i+aFromIndex];
      int bInt = b[i+bFromIndex];
      if (aInt > bInt) {
        return 1;
      } else if (aInt < bInt) {
        return -1;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
  }
  
  /**
   * Behaves like Java 9's Arrays.equals
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-int:A-int-int-int:A-int-int-">Arrays.equals</a>
   */
  public static boolean equals(int[] a, int aFromIndex, int aToIndex, int[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
      return false;
    }
    for (int i = 0; i < aLen; i++) {
      if (a[i+aFromIndex] != b[i+bFromIndex]) {
        return false;
      }
    }
    return true;
  }
  
  // long[]
  
  /**
   * Behaves like Java 9's Arrays.compare
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#compare-long:A-int-int-long:A-int-int-">Arrays.compare</a>
   */
  public static int compare(long[] a, int aFromIndex, int aToIndex, long[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = Math.min(aLen, bLen);
    for (int i = 0; i < len; i++) {
      long aInt = a[i+aFromIndex];
      long bInt = b[i+bFromIndex];
      if (aInt > bInt) {
        return 1;
      } else if (aInt < bInt) {
        return -1;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
  }
  
  /**
   * Behaves like Java 9's Arrays.equals
   * @see <a href="http://download.java.net/java/jdk9/docs/api/java/util/Arrays.html#equals-long:A-int-int-long:A-int-int-">Arrays.equals</a>
   */
  public static boolean equals(long[] a, int aFromIndex, int aToIndex, long[] b, int bFromIndex, int bToIndex) {
    checkFromToIndex(aFromIndex, aToIndex, a.length);
    checkFromToIndex(bFromIndex, bToIndex, b.length);
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
      return false;
    }
    for (int i = 0; i < aLen; i++) {
      if (a[i+aFromIndex] != b[i+bFromIndex]) {
        return false;
      }
    }
    return true;
  }
  
}
