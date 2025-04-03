package com.dist.simplekafka;

import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import org.junit.Test;

public class TopicChangeListenerTest extends ZookeeperTestHarness {

    @Test
    public void detectNewTopicCreation() {
        // Setup: Register 3 brokers in ZooKeeper with their network details
        // This metadata will be loaded by controller during election
        zookeeperClient.registerBroker(new Broker(0, "10.10.10.10", 8000));
        zookeeperClient.registerBroker(new Broker(1, "10.10.10.11", 8001));
        zookeeperClient.registerBroker(new Broker(2, "10.10.10.12", 8002));

        // Create a stub network handler to capture messages sent to brokers from the controller
        StubBrokerNetworkHandler stubNetworkHandler = new StubBrokerNetworkHandler(testConfig());

        //Create controller and elect it as leader
        ZkController controller = new ZkController(zookeeperClient, 1, stubNetworkHandler);
        controller.elect(); // Critical step:
        // 1. Becomes controller by creating znode
        // 2. Loads broker metadata from ZK
        // 3. Subscribes to ZK changes
        // 4. Controller is subscribing to Topic changes. So it will get invoked
        // when a new topic is created.
        // Controller is also subscribing to broker changes to get broker ip address and port to
        // communicate with.

        // Create a new topic using admin utility
        // This will:
        // 1. Create topic metadata in ZK
        // 2. Trigger TopicChangeHandler
        // 3. Controller uses broker metadata to send requests
        CreateTopicCommand adminUtil = new CreateTopicCommand(zookeeperClient, new ReplicaAssigner());
        adminUtil.createTopic("topic1", 2, 3);

        // Verify: Wait until all expected messages are sent to correct broker addresses
        // This only works because controller loaded broker metadata during elect()
        TestUtils.waitUntilTrue(()->{
            return stubNetworkHandler.getMessages().size() == 6;
        }, "Waiting for the messages to be detected");

        // At this point, controller has successfully:
        // 1. Detected the new topic via TopicChangeHandler
        // 2. Used broker metadata to know where to send requests
        // 3. Sent UpdateMetadata to all brokers (3 messages)
        // 4. Sent LeaderAndIsr to involved brokers (3 messages)
    }
}