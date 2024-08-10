package com.dist.simplekafka;

import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BrokerChangeListenerTest extends ZookeeperTestHarness {

    private ZkController controller;
    private BrokerNetworkHandler socketServer;

    @Before
    public void setUp() throws Exception {
        super.setUp(); // Assuming ZookeeperTestHarness has a setUp method
        socketServer = new StubBrokerNetworkHandler(config);
    }

    @Test
    public void shouldAddNewBrokerInformationToControllerOnChange() throws Exception {
        var zookeeperClient1 = new ZookeeperClient(config);
        zookeeperClient1.registerBroker(new Broker(0, "10.10.10.10", 8000));

        var zookeeperClient2 = new ZookeeperClient(config);
        zookeeperClient2.registerBroker(new Broker(1, "10.10.10.11", 8001));

        var zookeeperClient3 = new ZookeeperClient(config);
        zookeeperClient3.registerBroker(new Broker(2, "10.10.10.12", 8002));

        controller = new ZkController(zookeeperClient, config.getBrokerId(), socketServer);
        controller.startup();

        TestUtils.waitUntilTrue(() -> controller.getLiveBrokers().size() == 3,
                "Waiting for all brokers to get added", 1000, 100);

        assertEquals(3, controller.getLiveBrokers().size());
    }

    @Test
    public void shouldRemoveBrokerInformationFromController() throws Exception {

        //Node1
            var zookeeperClient1 = new ZookeeperClient(config);
            zookeeperClient1.registerBroker(new Broker(0, "10.10.10.10", 8000));
            controller = new ZkController(zookeeperClient1, config.getBrokerId(),
                    socketServer);
            controller.startup();


        //Node2
            var zookeeperClient2 = new ZookeeperClient(config);
            zookeeperClient2.registerBroker(new Broker(1, "10.10.10.11", 8001));



        TestUtils.waitUntilTrue(() -> controller.getLiveBrokers().size() == 2,
                "Waiting for brokers to get added", 1000, 100);

        //Node2 crash/disconnection.
        zookeeperClient2.close();

        TestUtils.waitUntilTrue(() -> controller.getLiveBrokers().size() == 1,
                "Waiting for broker to be removed", 5000, 100);

        assertEquals(1, controller.getLiveBrokers().size());
        assertFalse(controller.getLiveBrokers().stream().anyMatch(b -> b.id() == 1));
    }
}