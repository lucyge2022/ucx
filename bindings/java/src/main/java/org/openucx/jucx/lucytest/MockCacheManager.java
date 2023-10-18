package org.openucx.jucx.lucytest;

import static java.nio.file.StandardOpenOption.READ;

import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MockCacheManager {
  public ConcurrentHashMap<PageId, PageInfo> pageMetaStore = new ConcurrentHashMap<>();
  public UcpContext mGlobalContext;
  public ReentrantLock mInstanceLock = new ReentrantLock();
  public static Random mRandom = new Random();
  public static final long WORKER_PAGE_SIZE = 1*1024*1024L;

  public static class PageId {
    public String mFileId;
    public int mPageIdx;
    public PageId(String fileId, int pageIdx) {
      mFileId = fileId;
      mPageIdx = pageIdx;
    }

    @Override
    public String toString() {
      return "FileId:" + mFileId + ":PageIdx:" + mPageIdx;
    }
  }

  public static class PageInfo {
    public String mFilePath;
  }

  public MockCacheManager(UcpContext globalContext) {
    mGlobalContext = globalContext;
  }

  public static byte[] generateRandomData(int length) {
    byte[] bytes = new byte[length];
    mRandom.nextBytes(bytes);
    return bytes;
  }

  public void cache(String ufsPathFile, long length) {
//    for (int bytesGened = 0; bytesGened < length;) {
//      int genLen = Math.min((int)length - bytesGened, 4096);
//      byte[] data = generateRandomData(genLen);
//      bytesGened += genLen;
//    }
  }

  public Optional<UcpMemory> get(String absFilePath, int offset, int bytesToRead)
      throws IOException {
    File file = new File(absFilePath);
    Path filePath = file.toPath();
    FileChannel fileChannel = FileChannel.open(filePath, READ);
    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY,
        offset, bytesToRead);
    UcpMemory mmapedMemory = mGlobalContext.memoryMap(new UcpMemMapParams()
        .setAddress(UcxUtils.getAddress(buf))
        .setLength(bytesToRead).nonBlocking());
    return Optional.of(mmapedMemory);
  }
}
