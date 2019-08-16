package com.tencent.hermes.inverted.index;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;


public abstract class Terms {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected Terms() {
  }

  /** Returns an iterator that will step through all
   *  terms. This method will not return null. */
  public abstract TermsEnum iterator() throws IOException;


  /** Returns the number of documents that have at least one
   *  term for this field, or -1 if this measure isn't
   *  stored by the codec.  Note that, just like other term
   *  measures, this measure does not take deleted documents
   *  into account. */
  public abstract int getDocCount() throws IOException;
  
  /** Returns the number of terms for this field, or -1 if this 
   *  measure isn't stored by the codec. Note that, just like 
   *  other term measures, this measure does not take deleted 
   *  documents into account. */
  public abstract long size() throws IOException;

 

  /** Zero-length array of {@link Terms}. */
  public final static Terms[] EMPTY_ARRAY = new Terms[0];
  
  /** Returns the smallest term (in lexicographic order) in the field. 
   *  Note that, just like other term measures, this measure does not 
   *  take deleted documents into account.  This returns
   *  null when there are no terms. */
  public BytesRef getMin() throws IOException {
    return iterator().next();
  }

  /** Returns the largest term (in lexicographic order) in the field. 
   *  Note that, just like other term measures, this measure does not 
   *  take deleted documents into account.  This returns
   *  null when there are no terms. */
  @SuppressWarnings("fallthrough")
  public BytesRef getMax() throws IOException {
    long size = this.size();
    
    if (size == 0) {
      // empty: only possible from a FilteredTermsEnum...
      return null;
    } else if (size >= 0) {
      // try to seek-by-ord
      try {
        TermsEnum iterator = iterator();
        iterator.seekExact(size - 1);
        return iterator.term();
      } catch (UnsupportedOperationException e) {
        // ok
      }
    }
    
    // otherwise: binary search
    TermsEnum iterator = iterator();
    BytesRef v = iterator.next();
    if (v == null) {
      // empty: only possible from a FilteredTermsEnum...
      return v;
    }

    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.append((byte) 0);

    // Iterates over digits:
    while (true) {

      int low = 0;
      int high = 256;

      // Binary search current digit to find the highest
      // digit before END:
      while (low != high) {
        int mid = (low+high) >>> 1;
        scratch.setByteAt(scratch.length()-1, (byte) mid);
        if (iterator.seekCeil(scratch.get()) == TermsEnum.SeekStatus.END) {
          // Scratch was too high
          if (mid == 0) {
            scratch.setLength(scratch.length() - 1);
            return scratch.get();
          }
          high = mid;
        } else {
          // Scratch was too low; there is at least one term
          // still after it:
          if (low == mid) {
            break;
          }
          low = mid;
        }
      }

      // Recurse to next digit:
      scratch.setLength(scratch.length() + 1);
      scratch.grow(scratch.length());
    }
  }
  
  /** 
   * Expert: returns additional information about this Terms instance
   * for debugging purposes.
   */
  public Object getStats() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("impl=" + getClass().getSimpleName());
    sb.append(",size=" + size());
    sb.append(",docCount=" + this.getDocCount());
    return sb.toString();
  }
}
