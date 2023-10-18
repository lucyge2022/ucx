package org.openucx.jucx.lucytest;

import com.google.common.base.Preconditions;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Supplier;

public class UcxDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(UcxDataReader.class);
  public static final int PAGE_SIZE = 4096;

  InetSocketAddress mAddr;
  private static InetSocketAddress sLocalAddr = null;
  // make this a global, one per process only instance
  UcpWorker mWorker;
  UcpEndpoint mWorkerEndpoint;
  String mUfsPath;

  public UcxDataReader(InetSocketAddress addr, UcpWorker worker,
                       String ufsPath) {
    try {
      sLocalAddr = new InetSocketAddress(InetAddress.getLocalHost(),0);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    mAddr = addr;
    mWorker = worker;
    mUfsPath = ufsPath;
  }

  public void acquireServerConn() {
    if (mWorkerEndpoint != null) {
      return;
    }
    LOG.info("Acquiring server connection for {}", mAddr);
    mWorkerEndpoint = mWorker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setErrorHandler((ep, status, errorMsg) ->
                LOG.error("[ERROR] creating ep to remote:"
                    + mAddr + " errored out: " + errorMsg
                    + " status:" + status + ",ep:" + ep.toString()))
            .setSocketAddress(mAddr));
  }

  synchronized public int progressWorker() throws Exception {
    return mWorker.progress();
  }

  public void waitForRequest(UcpRequest ucpRequest) {
    while(!ucpRequest.isCompleted()) {
      try {
        progressWorker();
      } catch (Exception e) {
        LOG.error("Error progressing req:", e);
      }
    }
  }

  public int readInternal(long position, ByteBuffer buffer, int length) throws IOException {
    ReadRequest readRequest = new ReadRequest(mUfsPath, position, length);
    ByteBuffer buf = readRequest.toBuffer();
    buf.clear();
    long tag = ReadRequest.generateTag(sLocalAddr);
    UcpRequest sendRequest = mWorkerEndpoint.sendTaggedNonBlocking(buf, tag, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        LOG.info("ReadReq:{} sent.", readRequest);
      }

      public void onError(int ucsStatus, String errorMsg) {
        throw new UcxException(errorMsg);
      }
    });
    waitForRequest(sendRequest);
    // now wait to recv data
    Preconditions.checkArgument(buffer.isDirect(), "ByteBuffer must be direct buffer");
    int bytesRead = 0;
    ByteBuffer preamble = ByteBuffer.allocateDirect(16);
    TreeMap<Long, ByteBuffer> buffers = new TreeMap<>();
    preamble.clear();
    while (bytesRead < length) {
      UcpRequest recvReq = mWorkerEndpoint.recvStreamNonBlocking(UcxUtils.getAddress(preamble), 16,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {}

            public void onError(int ucsStatus, String errorMsg) {
              throw new UcxException(errorMsg);
            }
          });
      waitForRequest(recvReq);
      preamble.clear();
      long seq = preamble.getLong();
      long size = preamble.getLong();
      preamble.clear();
      ByteBuffer seqBuffer = ByteBuffer.allocateDirect(8);
      ByteBuffer dataBuffer = ByteBuffer.allocateDirect((int)size);
      long[] addrs = new long[2];
      long[] sizes = new long[2];
      addrs[0] = UcxUtils.getAddress(seqBuffer);
      addrs[1] = UcxUtils.getAddress(dataBuffer);
      sizes[0] = 8;
      sizes[1] = size;
      recvReq = mWorkerEndpoint.recvStreamNonBlocking(addrs, sizes,
          UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              ByteBuffer seqBufView = UcxUtils.getByteBufferView(addrs[0], sizes[0]);
              seqBufView.clear();
              long sequence = seqBufView.getLong();
              ByteBuffer dataBufView = UcxUtils.getByteBufferView(addrs[1], sizes[1]);
              dataBufView.clear();
              LOG.info("Received buffers, seq:{}, data buf size:{}", sequence, sizes[1]);
              buffers.put(sequence, dataBufView);
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Error receiving buffers, seq:{}, data buf size:{}, errorMsg:{}",
                  seq, size, errorMsg);
              throw new UcxException(errorMsg);
            }
          });
      waitForRequest(recvReq);
      bytesRead += size;
    }
    while (!buffers.isEmpty()) {
      Map.Entry<Long, ByteBuffer> entry = buffers.pollFirstEntry();
      LOG.info("Copying seq:{},bufsize:{}", entry.getKey(), entry.getValue());
      entry.getValue().rewind();
      buffer.put(entry.getValue());
    }
    return 0;
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      System.out.println("Usage: UcxDataReader [remote_ip] [remote_port] [fileName] [offset] [length]");
      System.exit(-1);
    }
    String remoteIp = args[0];
    int remotePort = Integer.parseInt(args[1]);
    String fileName = args[2];
    int offset = Integer.parseInt(args[3]);
    int length = Integer.parseInt(args[4]);

    final UcpContext sGlobalContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
    final UcpWorker worker = sGlobalContext.newWorker(new UcpWorkerParams());
    UcxDataReader reader = new UcxDataReader(new InetSocketAddress(remoteIp, remotePort),
        worker, fileName);
    reader.acquireServerConn();
    ByteBuffer buf = ByteBuffer.allocateDirect(length);
    try {
      reader.readInternal(offset, buf, length);
    } catch (IOException e) {
      LOG.error("exception:e", e);
      throw new RuntimeException(e);
    }
    LOG.info("Starting ucp data reader...");

  }
}
