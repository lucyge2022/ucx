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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LucyTest {
  public static final String RANDOM_TEXT = "ABCDEFG-LUCY";
  public static final int MEM_SIZE = 1024;
  public static final String FILE_TO_SEND = "/root/testfile1";

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Need at least one arg");
      System.exit(-1);
    }
    LucyTest lucyTest = new LucyTest();
    if (args[0].equalsIgnoreCase("client")) {
      System.out.println("Start runTestClient...");
      lucyTest.runTestClient();
    } else if (args[0].equalsIgnoreCase("server")) {
      System.out.println("Start runTestServer...");
      lucyTest.runTestServer();
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
    UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
//    Path tempFile = Files.createTempFile("jucx", "test");
    Path tempFile = new File(FILE_TO_SEND).toPath();
    // 1. Create FileChannel to file in tmp directory.
    FileChannel fileChannel = FileChannel.open(tempFile, CREATE, WRITE, READ, DELETE_ON_CLOSE);
    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MEM_SIZE);
//    MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MEM_SIZE);
//    buf.asCharBuffer().put(RANDOM_TEXT);
//    buf.force();
    // 2. Register mmap buffer with ODP
    UcpMemory mmapedMemory = context.memoryMap(new UcpMemMapParams()
        .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());

//    assertEquals(mmapedMemory.getAddress(), UcxUtils.getAddress(buf));

    // 3. Test allocation
    UcpMemory allocatedMemory = context.memoryMap(new UcpMemMapParams()
        .allocate().setProtection(UcpConstants.UCP_MEM_MAP_PROT_LOCAL_READ)
        .setLength(MEM_SIZE).nonBlocking());
//    assertEquals(allocatedMemory.getLength(), MEM_SIZE);

    allocatedMemory.deregister();
    mmapedMemory.deregister();
    fileChannel.close();
    context.close();
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
      LucyTest.sendFile(acceptedRemoteEp);
    }
  }


  public static void sendFile(UcpEndpoint remoteEp) {
    UcpContext context = new UcpContext(new UcpParams().requestTagFeature());
    Path tempFile = new File(FILE_TO_SEND).toPath();
    FileChannel fileChannel = null;
    try {
      fileChannel = FileChannel.open(tempFile, CREATE, WRITE, READ, DELETE_ON_CLOSE);
      MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, MEM_SIZE);
      UcpMemory mmapedMemory = context.memoryMap(new UcpMemMapParams()
          .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());
      long tag = getTag(FILE_TO_SEND, 0);
      ReentrantLock lock = new ReentrantLock();
      Condition condition = lock.newCondition();
      UcpRequest sentReq = remoteEp.sendTaggedNonBlocking(mmapedMemory.getAddress(), MEM_SIZE,
        tag, new UcxCallback() {
            final long startTime = System.nanoTime();

            @Override
            public void onSuccess(UcpRequest request) {
              System.out.println("onSuccess! request completed:" + request.isCompleted());
              condition.signalAll();
            }

            @Override
            public void onError(int ucsStatus, String errorMsg) {
              logUtil(errorMsg + ",status:" + ucsStatus, null);
              condition.signalAll();
            }
          });
      try {
        lock.lock();
        condition.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      System.out.println("file sent, instant sentReq isCompleted:" + sentReq.isCompleted());
    } catch (IOException e) {
      logUtil("sendFile error", e);
    }
  }

  InetSocketAddress serverAddr_ = new InetSocketAddress("127.0.0.1", 0);
  long tagFileIdMask_ = 0xFFFFFFFFFFFF0000L;

  public void runTestServer() {
    UcpContext context = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature());
    UcpWorker worker = context.newWorker(new UcpWorkerParams());
    // rdma worker params for rdma Send/Receive with TAG matching?? check UcpEndpointTest.testSendRecv
//    UcpWorkerParams rdmaWorkerParams = new UcpWorkerParams().requestWakeupRMA();

    AtomicReference<UcpConnectionRequest> connRequest = new AtomicReference<>(null);

    // Get all NIC addrs
    List<InetAddress> addresses = getInterfaces().flatMap(iface ->
            Collections.list(iface.getInetAddresses()).stream())
        .filter(addr -> !addr.isLinkLocalAddress())
        .collect(Collectors.toList());

    UcpListenerParams listenerParams = new UcpListenerParams()
        .setSockAddr(serverAddr_)
        .setConnectionHandler(new LucyConnectionListener(worker));
    UcpListener listener = worker.newListener(listenerParams);
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
                    + serverAddr_ + " errored out: " + errorMsg))
            .setSocketAddress(serverAddr_));
    return epToServer;
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
    UcpContext context = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature());
    UcpWorker worker = context.newWorker(new UcpWorkerParams());
    // client will receive from server
    UcpEndpoint serverEp = connectToServer(worker);

    FileChannel fileChannel = null;
    String recvFileName = "/tmp/recv_file";
    try {
      fileChannel = FileChannel.open(Paths.get(recvFileName),
          EnumSet.of(StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)
      );
      MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MEM_SIZE);
      UcpMemory mmapedMemory = context.memoryMap(new UcpMemMapParams()
          .setAddress(UcxUtils.getAddress(buf)).setLength(MEM_SIZE).nonBlocking());
//      UcpRequestParams ucpRequestParams = new UcpRequestParams()
//          .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST);
      long tag = getTag(FILE_TO_SEND, 0);
      ReentrantLock lock = new ReentrantLock();
      Condition condition = lock.newCondition();
      UcpRequest recvReq = worker.recvTaggedNonBlocking(mmapedMemory.getAddress(), MEM_SIZE, tag, tagFileIdMask_,
          new UcxCallback() {
            final long startTime = System.nanoTime();

            @Override
            public void onSuccess(UcpRequest request) {
              System.out.println("onSuccess! request completed:" + request.isCompleted());
              condition.signalAll();
            }

            @Override
            public void onError(int ucsStatus, String errorMsg) {
              logUtil(errorMsg + ",status:" + ucsStatus, null);
              condition.signalAll();
            }
          });
      condition.await();
      System.out.println("Waked on waiting, request isCompleted:" + recvReq.isCompleted());
      mmapedMemory.deregister();
      context.close();
    } catch (IOException | InterruptedException e) {
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
