package org.openucx.jucx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.openucx.jucx.lucytest.LucyTest;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerConnectionHandler;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicReference;

public class UcpLucyTest {

  @Test
  public void testLucyTestClient() throws InterruptedException {
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        LucyTest lucyTestServer = new LucyTest();
        try {
          System.out.println("Start runTestServer...");
          lucyTestServer.runTestServer();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("Start runTestClient...");
        LucyTest lucyTestClient = new LucyTest();
//        lucyTestClient.runTestClient();
        lucyTestClient.runTestStreamClient();
      }
    });
    t1.start();
    Thread.sleep(5000);
    t2.start();
    t1.join();
    t2.join();
  }

  /**
   * Test if worker.recvTag on a particular tag from a remote client ep
   * if closing ep how will recvTag act
   */
  @Test
  public void testCloseConnection() throws Exception {
    UcpContext ucpContext = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
    UcpWorker worker = ucpContext.newWorker(new UcpWorkerParams());
    AtomicReference<UcpEndpoint> clientEpOnServer = new AtomicReference<>(null);
    UcpListener listener = worker.newListener(new UcpListenerParams()
        .setSockAddr(new InetSocketAddress("127.0.0.1", 0))
        .setConnectionHandler(
        new UcpListenerConnectionHandler() {
      @Override
      public void onConnectionRequest(UcpConnectionRequest connectionRequest) {
        UcpEndpoint acceptedEp = worker.newEndpoint(
            new UcpEndpointParams()
                .setErrorHandler((ep, status, errorMsg) -> {
                  System.out.println("error on client:" + connectionRequest.getClientAddress().toString()
                      + ",errMsg:" + errorMsg + ",status:" + status + ", closing clientEpOnServer...");
                  try {
                    clientEpOnServer.get().close();
                  } catch (Exception ex) {
                    System.out.println("clientEpOnServer close exception...");
                  }
                })
                .setPeerErrorHandlingMode()
                .setConnectionRequest(connectionRequest));
        clientEpOnServer.compareAndSet(null, acceptedEp);
      }
    }));
    System.out.println("listener on:" + listener.getAddress());

    UcpContext ucpContextClient = new UcpContext(new UcpParams()
        .requestStreamFeature()
        .requestTagFeature()
        .requestWakeupFeature());
    UcpWorker clientWorker = ucpContextClient.newWorker(new UcpWorkerParams());
    System.out.println("client start connect to server addr:" + listener.getAddress());
    UcpEndpoint serverEpOnClient = clientWorker.newEndpoint(
        new UcpEndpointParams()
            .setPeerErrorHandlingMode()
            .setSocketAddress(listener.getAddress()));

    while(clientEpOnServer.get() == null) {
      System.out.println("client not connected, progressing server worker and client worker..");
      try {
        worker.progress();
        clientWorker.progress();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    ByteBuffer buf = ByteBuffer.allocateDirect(8);
    buf.putInt(1234);
    buf.clear();
    UcpRequest sendTagReq = serverEpOnClient.sendTaggedNonBlocking(buf, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        System.out.println("Tag sent from client.");
      }

      public void onError(int ucsStatus, String errorMsg) {
        System.out.println("Tag sent from client errored.");
        throw new UcxException(errorMsg);
      }
    });
    ByteBuffer recvbuf = ByteBuffer.allocateDirect(8);
    buf.clear();
    UcpRequest recvTagReq = worker.recvTaggedNonBlocking(recvbuf, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        System.out.println("Tag recv-ed on server.");
      }

      public void onError(int ucsStatus, String errorMsg) {
        System.out.println("Tag recv-ed on server errored.");
        throw new UcxException(errorMsg);
      }
    });
    while (!sendTagReq.isCompleted() || !recvTagReq.isCompleted()) {
      try {
        clientWorker.progress();
        worker.progress();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("closing client resources...");
    Stack<Closeable> resources = new Stack<>();
    Collections.addAll(resources, ucpContextClient, clientWorker);
    while (!resources.empty()) {
      try {
        resources.pop().close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    recvbuf.clear();
    UcpRequest recvTagReqWhenClientClose = worker.recvTaggedNonBlocking(recvbuf, new UcxCallback() {
      public void onSuccess(UcpRequest request) {
        System.out.println("TagNext recv-ed on server.");
      }

      public void onError(int ucsStatus, String errorMsg) {
        System.out.println("TagNext recv-ed on server errored.");
        throw new UcxException(errorMsg);
      }
    });
    while(true) {
      try {
        while (worker.progress() == 0) {
          System.out.println("Nothing to progress, waiting for events...");
          worker.waitForEvents();
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
