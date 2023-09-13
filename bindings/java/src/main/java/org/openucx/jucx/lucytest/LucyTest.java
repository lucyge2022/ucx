package org.openucx.jucx.lucytest;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpRequestParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LucyTest {
  public static final String RANDOM_TEXT = "ABCDEFG-LUCY";
  public static final int MEM_SIZE = 64;
  public static final String FILE_TO_SEND = "/root/testfile1";
  public static UcpContext globalContext_;

  public LucyTest() {
    globalContext_ = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Need at least one arg");
      System.exit(-1);
    }
    try {
      LucyTest lucyTest = new LucyTest();
      if (args[0].equalsIgnoreCase("client")) {
        System.out.println("Start runTestClient...");
        lucyTest.runTestClient();
      } else if (args[0].equalsIgnoreCase("server")) {
        System.out.println("Start runTestServer...");
        lucyTest.runTestServer();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    System.exit(-1);
  }

  public static void logUtil(String errMsg, Throwable th) {
    System.out.println(errMsg);
    if (th != null) {
      th.printStackTrace();
    }
  }

  public void openFile() throws IOException {
//    Path tempFile = Files.createTempFile("jucx", "test");
    Path tempFile = new File(FILE_TO_SEND).toPath();
    // 1. Create FileChannel to file in tmp directory.
    FileChannel fileChannel = FileChannel.open(tempFile, CREATE, WRITE, READ, DELETE_ON_CLOSE);
    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MEM_SIZE);
//    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MEM_SIZE);
//    buf.asCharBuffer().put(RANDOM_TEXT);
//    buf.force();
    // 2. Register mmap buffer with ODP
    UcpMemory mmapedMemory = globalContext_.memoryMap(new UcpMemMapParams()
        .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());

//    assertEquals(mmapedMemory.getAddress(), UcxUtils.getAddress(buf));

    // 3. Test allocation
    UcpMemory allocatedMemory = globalContext_.memoryMap(new UcpMemMapParams()
        .allocate().setProtection(UcpConstants.UCP_MEM_MAP_PROT_LOCAL_READ)
        .setLength(MEM_SIZE).nonBlocking());
//    assertEquals(allocatedMemory.getLength(), MEM_SIZE);

    allocatedMemory.deregister();
    mmapedMemory.deregister();
    fileChannel.close();
    globalContext_.close();
  }


  public static Set<UcpEndpoint> acceptedEndpoints_ = new HashSet<>();

  public static class LucyConnectionListener implements UcpListenerConnectionHandler {
    public UcpWorker worker_;
    public LucyConnectionListener(UcpWorker worker) {
      worker_ = worker;
    }

    @Override
    public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
      UcpEndpoint acceptedRemoteEp = worker_.newEndpoint(
          new UcpEndpointParams()
              .setPeerErrorHandlingMode()
              .setConnectionRequest(connectionRequest));
      acceptedEndpoints_.add(acceptedRemoteEp);
      System.out.println("Connection from client established:"
          + acceptedRemoteEp.getRemoteAddress());
      LucyTest.sendFile(worker_, acceptedRemoteEp);
    }
  }


  public static void sendFile(UcpWorker worker,
                              UcpEndpoint remoteEp) {
    Path tempFile = new File(FILE_TO_SEND).toPath();
    FileChannel fileChannel = null;
    try {
      fileChannel = FileChannel.open(tempFile, READ);
      MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MEM_SIZE);
      UcpMemory mmapedMemory = globalContext_.memoryMap(new UcpMemMapParams()
          .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());
      long tag = getTag(FILE_TO_SEND, 0);
      UcpRequest sentReq = remoteEp.sendTaggedNonBlocking(mmapedMemory.getAddress(), MEM_SIZE,
        tag, new UcxCallback() {
            final long startTime = System.nanoTime();

            @Override
            public void onSuccess(UcpRequest request) {
              System.out.println("onSuccess! request completed:" + request.isCompleted());
            }

            @Override
            public void onError(int ucsStatus, String errorMsg) {
              logUtil(errorMsg + ",status:" + ucsStatus, null);
            }
          });
      while(!sentReq.isCompleted()) {
        try {
          System.out.println("start progressing sentReq");
          worker.progressRequest(sentReq);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println("file sent, instant sentReq isCompleted:" + sentReq.isCompleted());
      mmapedMemory.deregister();
    } catch (IOException e) {
      logUtil("sendFile error", e);
    }
  }

  InetSocketAddress serverAddr_ = new InetSocketAddress("172.31.21.70", 1234);
  long tagFileIdMask_ = 0xFFFFFFFFFFFF0000L;

  public void runTestServer() throws Exception {
    UcpWorker worker = globalContext_.newWorker(new UcpWorkerParams());
    // rdma worker params for rdma Send/Receive with TAG matching?? check UcpEndpointTest.testSendRecv
//    UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();

    AtomicReference<UcpConnectionRequest> connRequest = new AtomicReference<>(null);

    // Get all NIC addrs
    List<InetAddress> addresses = getInterfaces().flatMap(iface ->
            Collections.list(iface.getInetAddresses()).stream())
        .filter(addr -> !addr.isLinkLocalAddress())
        .collect(Collectors.toList());

    CompletableFuture<UcpEndpoint> connectedFut = new CompletableFuture<UcpEndpoint>();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(new UcpListenerConnectionHandler() {
          @Override
          public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
            UcpEndpoint acceptedRemoteEp = worker.newEndpoint(
                new UcpEndpointParams()
                    .setPeerErrorHandlingMode()
                    .setConnectionRequest(connectionRequest));
            acceptedEndpoints_.add(acceptedRemoteEp);
            System.out.println("Connection from client established:"
                + acceptedRemoteEp.getRemoteAddress());
            connectedFut.complete(acceptedRemoteEp);
          }
        });
    for (InetAddress addr : addresses) {
      UcpListener listener = worker.newListener(listenerParams
          .setSockAddr(new InetSocketAddress(addr, 1234)));
      System.out.println("Bound UcpListner on: " + listener.getAddress());
    }

    try {
      while (worker.progress() == 0) {
        System.out.println("nothing to progress");
        worker.waitForEvents();
      }
      UcpEndpoint remoteClientEp = connectedFut.get();
      System.out.println("Accepted remote ep connection, localAddr:"
          + remoteClientEp.getLocalAddress() + ", remoteAddr:"
          + remoteClientEp.getRemoteAddress());
      LucyTest.sendFile(worker, remoteClientEp);
    } catch (Exception e) {
      logUtil("exception in runTestServer", e);
      throw e;
    }
/*
    UcpEndpoint clientEndpoint = worker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setSocketAddress(listener.getAddress()));

    try {
      while (connRequest.get() == null) {
        worker.progress();
      }
    } catch (Exception e) {
      logUtil("error worker.progress:", e);
    }

    UcpEndpoint serverEndpoint = worker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setConnectionRequest(connRequest.get()));

    try {
      ByteBuffer buffer = ByteBuffer.allocateDirect(8);
      worker.progressRequest(clientEndpoint.sendStreamNonBlocking(buffer, null));
      worker.progressRequest(serverEndpoint.sendStreamNonBlocking(buffer, null));
      worker.progressRequest(clientEndpoint.recvStreamNonBlocking(buffer, 8, null));
      worker.progressRequest(serverEndpoint.recvStreamNonBlocking(buffer, 8, null));
    } catch (Exception e) {
      logUtil("worker.progressRequest:", e);
    }

    assert(0 != clientEndpoint.getLocalAddress().getPort());
    assert(0 != clientEndpoint.getRemoteAddress().getPort());
    assert("127.0.0.1".equals(clientEndpoint.getLocalAddress().getHostString()));
    assert("127.0.0.1".equals(clientEndpoint.getRemoteAddress().getHostString()));
*/
  }

  public UcpEndpoint connectToServer(UcpWorker worker) {
    // Create and connect an endpoint to remote.
    UcpEndpoint epToServer = worker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setErrorHandler((ep, status, errorMsg) ->
                System.out.println("[ERROR] creating ep to remote:"
                    + serverAddr_ + " errored out: " + errorMsg
                    + " status:" + status + ",ep:" + ep.toString()))
            .setSocketAddress(serverAddr_));
//    establishOOBConnection(epToServer, worker);
    return epToServer;
  }

  public boolean sendStream(UcpEndpoint remoteEp,
                            UcpWorker localWorker,
                            final long address, final long size,
                            final boolean blocking) {
    final UcpRequest request = remoteEp.sendStreamNonBlocking(address, size, null);
    while (blocking && !request.isCompleted()) {
      try {
        System.out.println("sendStream req not complete, keep progress req.");
        localWorker.progressRequest(request);
      } catch (Exception e) {
        // Should never happen, since we do no throw exceptions inside our error handlers
        throw new IllegalStateException(e);
      }
    }
    return request.isCompleted();
  }

  public boolean receiveStream(UcpEndpoint remoteEp,
                                       UcpWorker localWorker,
                                       final long address, final long size,
                                       final boolean blocking) {
    final UcpRequest request = remoteEp.recvStreamNonBlocking(
        address, size, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null);
    while (blocking && !request.isCompleted()) {
      try {
        localWorker.progressRequest(request);
      } catch (Exception e) {
        // Should never happen, since we do no throw exceptions inside our error handlers
        throw new IllegalStateException(e);
      }
    }
    return request.isCompleted();
  }

  public void establishOOBConnection(UcpEndpoint remoteEp,
                                     UcpWorker localWorker) {
    long dummyAddr = getTag("OOB_ESTABLISH_CONN", 0);
    ByteBuffer buf = ByteBuffer.allocateDirect(Long.BYTES);
    buf.putLong(dummyAddr);
    ByteBuffer recvBuf = ByteBuffer.allocateDirect(Long.BYTES);
    UcpMemory memory = globalContext_.registerMemory(buf);
    UcpMemory recvMemory = globalContext_.registerMemory(recvBuf);
    boolean sendSuccess = sendStream(remoteEp, localWorker, memory.getAddress(), memory.getLength(),true);
    System.out.println("sendSuccess:" + sendSuccess);
    boolean recvSuccess = receiveStream(remoteEp, localWorker, recvMemory.getAddress(), recvMemory.getLength(), true);
    recvBuf.flip();
    System.out.println(recvBuf.getLong());
    memory.deregister();
    recvMemory.deregister();
  }

  private static byte[] getMd5Hash(String object) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      md5.update(object.getBytes());
      return md5.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static long getTag(String fileName, int indexId) {
    byte[] md5bytes = getMd5Hash(fileName);
    ByteBuffer buf = ByteBuffer.wrap(md5bytes);
    long fileId = buf.getLong();
    long tag = fileId << 16;
    tag |= indexId;
    return tag;
  }

  public static UcxCallback ucxCallback_ = new UcxCallback() {
    final long startTime = System.nanoTime();

    @Override
    public void onSuccess(UcpRequest request) {
      System.out.println("onSuccess! request completed:" + request.isCompleted());
    }

    @Override
    public void onError(int ucsStatus, String errorMsg) {
      logUtil(errorMsg + ",status:" + ucsStatus, null);
    }
  };

  public void runTestClient() {
    UcpWorker worker = globalContext_.newWorker(new UcpWorkerParams());
    // client will receive from server
    UcpEndpoint serverEp = connectToServer(worker);
    System.out.println("connected: localaddr:" + serverEp.getLocalAddress()
        + " remote addr:" + serverEp.getRemoteAddress());

    FileChannel fileChannel = null;
    String recvFileName = "/tmp/recv_file";
    try {
      fileChannel = FileChannel.open(Paths.get(recvFileName),
          CREATE, WRITE, READ, DELETE_ON_CLOSE
      );
//      MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MEM_SIZE);
      ByteBuffer buf = ByteBuffer.allocateDirect(MEM_SIZE);
//      UcpMemory mmapedMemory = globalContext_.memoryMap(new UcpMemMapParams()
//          .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());
//      UcpRequestParams ucpRequestParams = new UcpRequestParams()
//          .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
      long tag = getTag(FILE_TO_SEND, 0);
//      ReentrantLock lock = new ReentrantLock();
//      Condition condition = lock.newCondition();
      UcpRequest recvReq = worker.recvTaggedNonBlocking(
          UcxUtils.getAddress(buf), MEM_SIZE, tag, tagFileIdMask_,
//          mmapedMemory.getAddress(), MEM_SIZE, tag, tagFileIdMask_,
          new UcxCallback() {
            final long startTime = System.nanoTime();

            @Override
            public void onSuccess(UcpRequest request) {
              System.out.println("onSuccess! request completed:" + request.isCompleted());
//              condition.signalAll();
            }

            @Override
            public void onError(int ucsStatus, String errorMsg) {
              logUtil(errorMsg + ",status:" + ucsStatus, null);
//              condition.signalAll();
            }
          });
      while(!recvReq.isCompleted()) {
        try {
          System.out.println("start progressing recvReq");
          worker.progressRequest(recvReq);
        } catch (Exception e) {
          logUtil("error progressing recvReq", e);
          throw new RuntimeException(e);
        }
      }
      System.out.println("Waked on waiting, request isCompleted:" + recvReq.isCompleted());
//      mmapedMemory.deregister();
      System.out.println("buf remaining:" + buf.remaining()
          + " buf position:" + buf.position()
          + " buf limit:" + buf.limit()
          + " buf capacity:" + buf.capacity());
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      System.out.println("Received buffer content:" + new String(bytes));
      globalContext_.close();
    } catch (IOException e) {
      logUtil("error open file channel to received file:", e);
    }
  }


  static Stream<NetworkInterface> getInterfaces() {
    try {
      return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(iface -> {
            try {
              return iface.isUp() && !iface.isLoopback() &&
                  !iface.isVirtual() &&
                  !iface.getName().contains("docker");
            } catch (SocketException e) {
              return false;
            }
          });
    } catch (SocketException e) {
      return Stream.empty();
    }
  }
}
