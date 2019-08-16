package com.tencent.hermes.inverted.index;


import java.io.IOException;
import java.util.Iterator;

/** Flex API for access to fields and terms
 *  @lucene.experimental */

public abstract class Fields implements Iterable<Field> {

  protected Fields() {
  }

  /** Returns an iterator that will step through all fields
   *  names.  This will not return null.  */
  @Override
  public abstract Iterator<Field> iterator();

  /** Get the {@link Terms} for this field.  This will return
   *  null if the field does not exist. */
  public abstract Terms terms(String field) throws IOException;

  /** Returns the number of fields or -1 if the number of
   * distinct field names is unknown. If &gt;= 0,
   * {@link #iterator} will return as many field names. */
  public abstract int size();
}
