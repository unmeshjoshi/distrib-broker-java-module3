package com.dist.common;

import com.dist.simplekafka.Server;
import com.dist.simplekafka.ZookeeperClient;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;

public class ZookeeperTestHarness {
    public static final String zkConnectAddress =
            "127.0.0.1:" + TestUtils.choosePort();
    //choose random port to allow multiple tests to run together

    protected final int zkConnectionTimeout = 10000;
    protected final int zkSessionTimeout = 15000;

    protected EmbeddedZookeeper zookeeper;
    protected ZookeeperClient zookeeperClient;
    protected Config config;


    @Before
    public void setUp() throws Exception {
        zookeeper = new EmbeddedZookeeper(zkConnectAddress);
        config = testConfig();
        zookeeperClient = new ZookeeperClient(config);

    }

    @After
    public void tearDown() {
        zookeeper.shutdown();
    }

    protected Config testConfig() {
        return new Config(1, new Networks().hostname(),
                TestUtils.choosePort(), zkConnectAddress,
                Collections.singletonList(TestUtils.tempDir().getAbsolutePath()));
    }

    protected static Server newBroker(int brokerId) {
        Config config = new Config(brokerId, new Networks().hostname(),
                TestUtils.choosePort(), zkConnectAddress,
                Arrays.asList(TestUtils.tempDir().getAbsolutePath()));


        Server server = Server.create(config);
        return server;
    }

}
