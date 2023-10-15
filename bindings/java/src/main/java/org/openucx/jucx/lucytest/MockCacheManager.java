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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MockCacheManager {
  public ConcurrentHashMap<PageId, PageInfo> pageMetaStore = new ConcurrentHashMap<>();
  public UcpContext mGlobalContext;
  public ReentrantLock mInstanceLock = new ReentrantLock();

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

  public Optional<UcpMemory> get(PageId pageId, int pageOffset, int bytesToRead)
      throws IOException {
    PageInfo pageInfo = pageMetaStore.get(pageId);
    if (pageInfo == null) {
      throw new IOException("No such page : " + pageId);
    }
    Path filePath = new File(pageInfo.mFilePath).toPath();
    FileChannel fileChannel = FileChannel.open(filePath, READ);
    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY,
        pageOffset, bytesToRead);
    UcpMemory mmapedMemory = mGlobalContext.memoryMap(new UcpMemMapParams()
        .setAddress(UcxUtils.getAddress(buf))
        .setLength(bytesToRead).nonBlocking());
    return Optional.of(mmapedMemory);
  }
}
