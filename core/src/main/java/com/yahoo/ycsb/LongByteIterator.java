package com.yahoo.ycsb;

/**
 * A class to use long values.
 */
public class LongByteIterator extends ByteIterator {

  private long l;

  public LongByteIterator(long l) {
    this.l = l;
  }

  public long getValue() {
    return l;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public byte nextByte() {
    return 0;
  }

  @Override
  public long bytesLeft() {
    return 0;
  }
}
