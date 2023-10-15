package org.openucx.jucx.lucytest;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

public class ReadRequest {
  private long mOffset;
  private long mLength;
  private String mUfsPath;

  public ReadRequest(String ufsPath, long offset, long length) {
    mUfsPath = ufsPath;
    mOffset = offset;
    mLength = length;
  }

  public static String hash(String uri) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(uri.getBytes());
      return DatatypeConverter
          .printHexBinary(md.digest()).toLowerCase();
//      return Hex.encodeHexString(md.digest()).toLowerCase();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
      /* No actions. Continue with other hash method. */
    }
  }

  public static long generateTag(InetSocketAddress inetSocketAddress) {
    // first 16 empty, then ipv4 = 8*4 = 32 bit then port = 16 bit
    long tag = 0L;
    InetAddress remoteAddr = inetSocketAddress.getAddress();
    if (remoteAddr instanceof Inet4Address) {
      byte[] ip = ((Inet4Address)remoteAddr).getAddress();
      tag |= ((long)0xFF & ip[0]) << 24;
      tag |= ((long)0xFF & ip[1]) << 16;
      tag |= ((long)0xFF & ip[2]) << 8;
      tag |= (long)0xFF & ip[3];
      tag = tag << 16;
    }
    tag |= inetSocketAddress.getPort();
    return tag;
  }

  public static ReadRequest parseFrom(ByteBuffer buf) {
    long offSet = buf.getLong();
    long length = buf.getLong();
    int strLen = buf.getInt();
    byte[] str = new byte[strLen];
    buf.get(str);
    String ufsPath = new String(str);
    return new ReadRequest(ufsPath, offSet, length);
  }

  public String getUfsPath() {
    return mUfsPath;
  }

  public long getOffset() {
    return mOffset;
  }

  public long getLength() {
    return mLength;
  }
}
