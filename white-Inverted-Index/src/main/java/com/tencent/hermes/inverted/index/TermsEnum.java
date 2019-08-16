package com.tencent.hermes.inverted.index;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;


public abstract class TermsEnum implements BytesRefIterator {




  protected TermsEnum() {
  }


  /** Represents returned result from {@link #seekCeil}. */
  public static enum SeekStatus {
    /** The term was not found, and the end of iteration was hit. */
    END,
    /** The precise term was found. */
    FOUND,
    /** A different term was found after the requested term */
    NOT_FOUND
  };

  /** Attempts to seek to the exact term, returning
   *  true if the term is found.  If this returns false, the
   *  enum is unpositioned.  For some codecs, seekExact may
   *  be substantially faster than {@link #seekCeil}. */
  public boolean seekExact(BytesRef text) throws IOException {
    return seekCeil(text) == SeekStatus.FOUND;
  }

  /** Seeks to the specified term, if it exists, or to the
   *  next (ceiling) term.  Returns SeekStatus to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be before or after the current term.  If this returns
   *  SeekStatus.END, the enum is unpositioned. */
  public abstract SeekStatus seekCeil(BytesRef text) throws IOException;

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be before or after the current ord, and must be
   *  within bounds. */
  public abstract void seekExact(long ord) throws IOException;


  /** Returns current term. Do not call this when the enum
   *  is unpositioned. */
  public abstract BytesRef term() throws IOException;

  /** Returns ordinal position for current term.  This is an
   *  optional method (the codec may throw {@link
   *  UnsupportedOperationException}).  Do not call this
   *  when the enum is unpositioned. */
  public abstract long ord() throws IOException;



  
  public static final TermsEnum EMPTY = new TermsEnum() {    
    @Override
    public SeekStatus seekCeil(BytesRef term) { return SeekStatus.END; }
    
    @Override
    public void seekExact(long ord) {}
    
    @Override
    public BytesRef term() {
      throw new IllegalStateException("this method should never be called");
    }

	@Override
	public BytesRef next() throws IOException {
		return null;
	}

	@Override
	public long ord() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	} 

  };
}
