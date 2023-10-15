package org.openucx.jucx.lucytest;

//import alluxio.AlluxioURI;
//import alluxio.client.file.cache.CacheManagerOptions;
//import alluxio.client.file.cache.LocalCacheManager;
//import alluxio.client.file.cache.PageId;
//import alluxio.client.file.cache.PageMetaStore;
//import alluxio.conf.Configuration;
//import alluxio.exception.PageNotFoundException;
//import alluxio.proto.dataserver.Protocol;
//import alluxio.ucx.UcpUtils;
//import alluxio.util.ThreadFactoryUtils;
//
//import alluxio.util.io.BufferPool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.core.net.Protocol;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UcpServer {
  public static final int PAGE_SIZE = 4096;

  private static final Logger LOG = LoggerFactory.getLogger(UcpServer.class);
  private MockCacheManager mlocalCacheManager;
  private UcpWorker mGlobalWorker;
  private Map<PeerInfo, UcpEndpoint> mPeerEndpoints = new ConcurrentHashMap<>();
  // TODO(lucy) backlogging if too many incoming req...
  private LinkedBlockingQueue<UcpConnectionRequest> mConnectionRequests
      = new LinkedBlockingQueue<>();
  private LinkedBlockingQueue<UcpRequest> mReceiveRequests
      = new LinkedBlockingQueue<>();
  private static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestWakeupFeature());
  private int BIND_PORT = 1234;

  private AcceptorThread mAcceptorLoopThread;

  private ThreadPoolExecutor tpe = new ThreadPoolExecutor(4, 4,
      0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(4),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("worker-threads-%d").build());

  private ExecutorService mAcceptorExecutor =  Executors.newFixedThreadPool(1);

  public UcpServer() throws IOException {
    mlocalCacheManager = new MockCacheManager(sGlobalContext);
    mGlobalWorker = sGlobalContext.newWorker(new UcpWorkerParams());
    List<InetAddress> addressesToBind = getAllAddresses();
    UcpListenerParams listenerParams = new UcpListenerParams()
        .setConnectionHandler(new UcpListenerConnectionHandler() {
          @Override
          public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
            LOG.info("Incoming request, clientAddr:{} clientId:{}",
                connectionRequest.getClientAddress(), connectionRequest.getClientId());
            mConnectionRequests.offer(connectionRequest);
          }
        });
    for (InetAddress addr : addressesToBind) {
      UcpListener ucpListener = mGlobalWorker.newListener(listenerParams.setSockAddr(
          new InetSocketAddress(addr, BIND_PORT)));
      LOG.info("Bound UcpListener on address:{}", ucpListener.getAddress());
    }
    mAcceptorExecutor.submit(new AcceptorThread());
  }

  public void awaitTermination() {
    mAcceptorExecutor.shutdown();
  }

  public static void main(String[] args) {
    try {
      LOG.info("Starting ucp server...");
      UcpServer ucpServer = new UcpServer();
      LOG.info("Awaiting termination...");
      ucpServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static class PeerInfo {
    InetSocketAddress mRemoteAddr;
    long mClientId;
    public PeerInfo(InetSocketAddress remoteAddr, long clientId) {
      mRemoteAddr = remoteAddr;
      mClientId = clientId;
    }

    public static byte[] serialize(PeerInfo peerInfo) throws IOException {
      try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
        try (ObjectOutputStream o = new ObjectOutputStream(b)) {
          o.writeObject(peerInfo);
        }
        return b.toByteArray();
      }
    }

    public static PeerInfo parsePeerInfo(ByteBuffer recvBuffer) throws IOException {
      // Need a common util of serialization
      recvBuffer.clear();
      int peerInfoLen = recvBuffer.getInt();
      byte[] peerInfoBytes = new byte[peerInfoLen];
      recvBuffer.get(peerInfoBytes);
      try (ByteArrayInputStream b = new ByteArrayInputStream(peerInfoBytes)) {
        try (ObjectInputStream o = new ObjectInputStream(b)) {
          return (PeerInfo) o.readObject();
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public String toString() {
      return "PeerInfo:addr:" + mRemoteAddr.toString()
          + ":clientid:" + mClientId;
    }
  }

  public static List<InetAddress> getAllAddresses() {
    // Get all NIC addrs
    Stream<NetworkInterface> nics = Stream.empty();
    try {
      nics = Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
          .filter(iface -> {
            try {
              return iface.isUp() && !iface.isLoopback() &&
                  !iface.isVirtual() &&
                  !iface.getName().contains("docker");
              // identify infiniband usually interface name looks like ib-...
            } catch (SocketException e) {
              return false;
            }
          });
    } catch (SocketException e) {
    }
    List<InetAddress> addresses = nics.flatMap(iface ->
            Collections.list(iface.getInetAddresses()).stream())
        .filter(addr -> !addr.isLinkLocalAddress())
        .collect(Collectors.toList());
    return addresses;
  }


  public static ReadRequest parseReadRequest(ByteBuffer buf)
      throws InvalidProtocolBufferException {
    int contentLen = buf.getInt();
    buf.limit(buf.position() + contentLen);
    ReadRequest request = ReadRequest.parseFrom(buf);
    return request;
  }

  class RPCMessageHandler implements Runnable {
    private static final long WORKER_PAGE_SIZE = 1*1024*1024L;
    ReadRequest readRequest = null;
    UcpEndpoint remoteEp;
    //conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);

    @Override
    public void run() {
      final String fileId = ReadRequest.hash(readRequest.getUfsPath());
      int pageIndex = (int)(readRequest.getOffset() / WORKER_PAGE_SIZE);
      int pageOffset = (int)(readRequest.getOffset() % WORKER_PAGE_SIZE);
      long totalLength = readRequest.getLength();
      MockCacheManager.PageId pageId = new MockCacheManager.PageId(fileId, pageIndex);
      for (int bytesRead = 0; bytesRead < totalLength; ) {
        int readLen = (int)Math.min(totalLength - bytesRead, WORKER_PAGE_SIZE);
        try {
          Optional<UcpMemory> readContentUcpMem =
              mlocalCacheManager.get(pageId, pageOffset, readLen);
          if (!readContentUcpMem.isPresent()) {
            break;
          }
          UcpRequest sendReq = remoteEp.sendStreamNonBlocking(
              readContentUcpMem.get().getAddress(),
              readContentUcpMem.get().getLength(), new UcxCallback() {
                public void onSuccess(UcpRequest request) {
                  LOG.info("send complete for pageid:{}:pageoffset:{}:readLen:{}",
                      pageId, pageOffset, readLen);
                }

                public void onError(int ucsStatus, String errorMsg) {
                  LOG.error("error sending:pageid:{}:pageoffset:{}:readLen:{}",
                      pageId, pageOffset, readLen);
                }
              });
        } catch ( IOException e) {
          throw new RuntimeException(e);
        }
        LOG.info("Handle read req:{} complete", readRequest);
      }
    }


    public RPCMessageHandler(PeerInfo peerInfo, ByteBuffer recvBuffer) {
      // deserialize rpc mesg
      readRequest = null;
      remoteEp = mPeerEndpoints.get(peerInfo);
      if (remoteEp == null) {
        throw new RuntimeException("unrecognized peerinfo:" + peerInfo.toString());
      }
      try {
        readRequest = parseReadRequest(recvBuffer);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // accept one single rpc req at a time
  class AcceptorThread implements Runnable {

    public UcpRequest recvRequest(PeerInfo peerInfo) {
      if (peerInfo == null) {
        return null;
      }
//      ByteBuffer recvBuffer = BufferPool.getInstance().getABuffer(PAGE_SIZE, true)
//          .nioBuffer();
      ByteBuffer recvBuffer = ByteBuffer.allocateDirect(PAGE_SIZE);
      LOG.info("recvBuffer capacity:{}:limit:{}:position:{}",
          recvBuffer.capacity(), recvBuffer.limit(), recvBuffer.position());
      recvBuffer.clear();
      long tag = ReadRequest.generateTag(peerInfo.mRemoteAddr);
      // currently only matching the ip, excluding port as ucx 1.15.0 doesn't expose ucpendpoint.getloaladdress
      // when client establish remote conn
      long tagMask = 0xFFFFFFFF0000L;
      UcpRequest recvRequest = mGlobalWorker.recvTaggedNonBlocking(
          recvBuffer, tag, tagMask, new UcxCallback() {
            public void onSuccess(UcpRequest request) {
              LOG.info("New req received from peer:{}", peerInfo);
              tpe.execute(new RPCMessageHandler(peerInfo, recvBuffer));
              LOG.info("onSuccess start receiving another req for peer:{}", peerInfo);
              recvRequest(peerInfo);
            }

            public void onError(int ucsStatus, String errorMsg) {
              LOG.error("Receive req errored, status:{}, errMsg:{}",
                  ucsStatus, errorMsg);
              LOG.info("onError start receiving another req for peer:{}", peerInfo);
              recvRequest(peerInfo);
            }
          });
      return recvRequest;
    }

    public void acceptNewConn() {
      UcpConnectionRequest connectionReq = mConnectionRequests.poll();
      if (connectionReq != null) {
        PeerInfo peerInfo = new PeerInfo(
            connectionReq.getClientAddress(), connectionReq.getClientId());
        boolean newConn = true;
        if (mPeerEndpoints.containsKey(peerInfo)) {
          newConn = false;
          LOG.warn("existing conn...");
        }
        mPeerEndpoints.compute(peerInfo, (pInfo, ep) -> {
          if (ep == null) {
            return mGlobalWorker.newEndpoint(new UcpEndpointParams()
                .setPeerErrorHandlingMode()
                .setConnectionRequest(connectionReq));
          } else {
            LOG.info("Endpoint for peer:{} already exist, rejecting connection req...", peerInfo);
            connectionReq.reject();
            return ep;
          }
        });
        if (newConn) {
          UcpRequest req = recvRequest(peerInfo);
          mReceiveRequests.offer(req);
        }
      }
    }

    @Override
    public void run() {
      try {
        while (true) {
          acceptNewConn();
          while (mGlobalWorker.progress() == 0) {
            LOG.info("nothing to progress. wait for events..");
            try {
              mGlobalWorker.waitForEvents();
            } catch (Exception e) {
              LOG.error(e.getLocalizedMessage());
            }
          }
        }
      } catch (Exception e) {
        // not sure what exception would be thrown here.
        LOG.info("Exception in AcceptorThread", e);
      }
    }
  }
}