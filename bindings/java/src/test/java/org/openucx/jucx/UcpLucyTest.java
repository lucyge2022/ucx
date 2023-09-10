package org.openucx.jucx;

import org.junit.Test;
import org.openucx.jucx.lucytest.LucyTest;

public class UcpLucyTest {

  @Test
  public void testLucyTestClient() {
    LucyTest lucyTestServer = new LucyTest();
    LucyTest lucyTestClient = new LucyTest();
    lucyTestServer.runTestServer();
    lucyTestServer.runTestClient();
  }
}
