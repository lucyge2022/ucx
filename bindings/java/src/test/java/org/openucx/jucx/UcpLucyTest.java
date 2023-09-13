package org.openucx.jucx;

import org.junit.Test;
import org.openucx.jucx.lucytest.LucyTest;

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
        lucyTestClient.runTestClient();
      }
    });
    t1.start();
    Thread.sleep(5000);
    t2.start();
    t1.join();
    t2.join();
  }
}
