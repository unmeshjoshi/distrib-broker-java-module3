package com.dist.simplekafka;

import com.dist.common.Config;
import com.dist.common.Networks;
import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
public class ZkControllerTest extends ZookeeperTestHarness {

    @Test
    public void singleNodeBecomesController() {
        var broker = new Broker(config.getBrokerId(), config.getHostName(), config.getPort());
        zookeeperClient.registerBroker(broker); //this will happen together
        // with controller initialization on each node.

        ZkController zkController = new ZkController(zookeeperClient,
                config.getBrokerId(), new StubBrokerNetworkHandler(config));

        zkController.elect();

        assertEquals(config.getBrokerId(), zkController.getCurrentLeaderId());
    }

    @Test
    public void forMultipleNodesOnlyOneBecomesController() {
        var broker = new Broker(config.getBrokerId(), config.getHostName(), config.getPort());
        zookeeperClient.registerBroker(broker); //this will happen together
        // with controller initialization on each node.

        ZkController zkController1 = new ZkController(zookeeperClient, 1, new StubBrokerNetworkHandler(config));
        ZkController zkController2 = new ZkController(zookeeperClient, 2, new StubBrokerNetworkHandler(config));
        ZkController zkController3 = new ZkController(zookeeperClient, 3, new StubBrokerNetworkHandler(config));

        zkController1.elect();
        zkController2.elect();
        zkController3.elect();



        assertEquals(1, zkController1.getCurrentLeaderId());
        assertEquals(1, zkController2.getCurrentLeaderId());
        assertEquals(1, zkController3.getCurrentLeaderId());


    }
    @Test
    public void shouldSendLeaderAndFollowerRequestsToAllLeaderAndFollowerBrokersForGivenTopicAndPartition() throws Exception {
        Config config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        ZookeeperClient zookeeperClient = new ZookeeperClient(config1);
        zookeeperClient.registerBroker(new Broker(config1.getBrokerId(), config1.getHostName(), config1.getPort()));

        StubBrokerNetworkHandler networkHandler = new StubBrokerNetworkHandler(config1);
        ZkController controller = new ZkController(zookeeperClient, config1.getBrokerId(), networkHandler);
        controller.startup();

        Config config2 = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        zookeeperClient.registerBroker(new Broker(config2.getBrokerId(), config2.getHostName(), config2.getPort()));

        Config config3 = new Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        zookeeperClient.registerBroker(new Broker(config3.getBrokerId(), config3.getHostName(), config3.getPort()));

        TestUtils.waitUntilTrue(() -> controller.getLiveBrokers().size() == 3
                , "Waiting for all brokers to get added", 1000, 100);

        assertEquals(3, controller.getLiveBrokers().size());

        CreateTopicCommand createCommandTest =
                new CreateTopicCommand(zookeeperClient,
                        new ReplicaAssigner(new Random()));
        createCommandTest.createTopic("topic1", 2, 1);

        TestUtils.waitUntilTrue(() -> networkHandler.getMessages().size() == 5 && networkHandler.getToAddresses().size() == 3,
                "waiting for leader and replica requests handled in all " +
                        "brokers", 2000, 100);

        networkHandler.getMessages().stream()
                .filter(m -> m.getRequestId() == RequestKeys.LeaderAndIsrKey)
                .forEach(message -> assertEquals(RequestKeys.LeaderAndIsrKey, message.getRequestId()));

        Set<InetAddressAndPort> expectedAddresses = Set.of(
                InetAddressAndPort.create(config1.getHostName(), config1.getPort()),
                InetAddressAndPort.create(config2.getHostName(), config2.getPort()),
                InetAddressAndPort.create(config3.getHostName(), config3.getPort())
        );

        assertEquals(expectedAddresses, networkHandler.getToAddresses());
    }

    @Test
    public void shouldSendUpdateMetadataRequestsToAllBrokers() throws Exception {
        Config config1 = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        ZookeeperClient zookeeperClient = new ZookeeperClient(config1);
        zookeeperClient.registerBroker(new Broker(config1.getBrokerId(), config1.getHostName(), config1.getPort()));

        StubBrokerNetworkHandler networkHandler = new StubBrokerNetworkHandler(config1);
        ZkController controller = new ZkController(zookeeperClient, config1.getBrokerId(), networkHandler);
        controller.startup();

        Config config2 = new Config(2, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        zookeeperClient.registerBroker(new Broker(config2.getBrokerId(), config2.getHostName(), config2.getPort()));

        Config config3 = new Config(3, new Networks().hostname(), TestUtils.choosePort(), zkConnectAddress, List.of(TestUtils.tempDir().getAbsolutePath()));
        zookeeperClient.registerBroker(new Broker(config3.getBrokerId(), config3.getHostName(), config3.getPort()));

        TestUtils.waitUntilTrue(() -> controller.getLiveBrokers().size() == 3
                , "Waiting for all brokers to get added", 1000, 100);

        assertEquals(3, controller.getLiveBrokers().size());

        CreateTopicCommand createCommandTest =
                new CreateTopicCommand(zookeeperClient,new ReplicaAssigner());
        createCommandTest.createTopic("topic1", 2, 3);

        TestUtils.waitUntilTrue(() -> networkHandler.getMessages().size() == 6 && networkHandler.getToAddresses().size() == 3,
                "waiting for leader and replica requests handled in all " +
                        "brokers", 2000, 100);

        networkHandler.getMessages().stream()
                .filter(m -> m.getRequestId() == RequestKeys.UpdateMetadataKey)
                .forEach(message -> assertEquals(RequestKeys.UpdateMetadataKey, message.getRequestId()));

        Set<InetAddressAndPort> expectedAddresses = Set.of(
                InetAddressAndPort.create(config1.getHostName(), config1.getPort()),
                InetAddressAndPort.create(config2.getHostName(), config2.getPort()),
                InetAddressAndPort.create(config3.getHostName(), config3.getPort())
        );

        assertEquals(expectedAddresses, networkHandler.getToAddresses());
    }
    
}