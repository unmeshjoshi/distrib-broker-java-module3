package com.dist.simplekafka;

import com.dist.common.TestUtils;
import com.dist.common.ZookeeperTestHarness;
import org.I0Itec.zkclient.IZkChildListener;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ZookeeperClientTest extends ZookeeperTestHarness {

    @Test
    public void registersBroker() {
        Broker broker = new Broker(1, "10.10.10.10", 8000);

        zookeeperClient.registerBroker(broker);
        Broker brokerInfo = zookeeperClient.getBrokerInfo(1);
        assertEquals(broker, brokerInfo);
    }

    @Test
    public void registersTopicPartitionInfo() {
        // Create a sample PartitionReplicas
        PartitionReplicas partitionReplica = new PartitionReplicas(1, List.of(1, 2, 3));
        List<PartitionReplicas> expectedPartitionReplicas =
                List.of(partitionReplica);

        // Register partition replicas
        zookeeperClient.setPartitionReplicasForTopic("topicName", expectedPartitionReplicas);

        // Retrieve and verify partition replicas
        List<PartitionReplicas> actualPartitionReplicas =
                zookeeperClient.getPartitionAssignmentsFor("topicName");
        assertEquals(expectedPartitionReplicas, actualPartitionReplicas);
    }

    @Test //Implement and Run this test...
    public void testSubscribeTopicChangeListener() throws InterruptedException {
        List<String> topicChildren = new ArrayList<>();
        IZkChildListener listener =
                (parentPath, currentChildren) -> topicChildren.addAll(currentChildren);


        zookeeperClient.subscribeTopicChangeListener(listener);

        // Create a new topic
        PartitionReplicas partitionReplica = new PartitionReplicas(1, List.of(1, 2, 3));
        List<PartitionReplicas> expectedPartitionReplicas =
                List.of(partitionReplica);
        zookeeperClient.setPartitionReplicasForTopic("newTopic", expectedPartitionReplicas);

        // Wait for the notification
        TestUtils.waitUntilTrue(() -> {
            return 1 == topicChildren.size();
        }, "There should be one notification");

        assertEquals(1, topicChildren.size());
        assertEquals("newTopic", topicChildren.get(0));
        List<PartitionReplicas> retrievedReplicas =
                zookeeperClient.getPartitionAssignmentsFor(topicChildren.get(0));
        assertEquals(expectedPartitionReplicas, retrievedReplicas);
    }

    @Test
    public void testSubcribeBrokerChangeListener() {
        List<String> topicChildren = new ArrayList<>();
        IZkChildListener listener =
                (parentPath, currentChildren) -> topicChildren.addAll(currentChildren);

        zookeeperClient.subscribeBrokerChangeListener(listener);

        zookeeperClient.registerBroker(new Broker(1, "10.10.10.10", 8000));

        TestUtils.waitUntilTrue(()->{
            return topicChildren.size() == 1;
        }, "Waiting for getting broker added notification");

        assertEquals("1", topicChildren.get(0));
    }

    @Test
    public void testGetTopics() {
        zookeeperClient.setPartitionReplicasForTopic("topic1",
                Collections.emptyList());
        zookeeperClient.setPartitionReplicasForTopic("topic2", Collections.emptyList());

        List<String> topics = zookeeperClient.getTopics();

        assertEquals(2, topics.size());
        assertTrue(topics.containsAll(Arrays.asList("topic1", "topic2")));
    }

    @Test
    public void testGetAllBrokerIds() {
        zookeeperClient.registerSelf();
        Set<Integer> brokerIds = zookeeperClient.getAllBrokerIds();
        assertEquals(1, brokerIds.size());
        assertTrue(brokerIds.contains(config.getBrokerId()));
    }

    @Test
    public void testGetAllBrokers() {
        zookeeperClient.registerSelf();
        Set<Broker> brokers = zookeeperClient.getAllBrokers();
        assertEquals(1, brokers.size());
        Broker broker = brokers.iterator().next();
        assertEquals(config.getBrokerId(), broker.id());
        assertEquals(config.getHostName(), broker.host());
        assertEquals(config.getPort(), broker.port());
    }

    @Test
    public void testGetBrokerInfo() {
        zookeeperClient.registerSelf();
        Broker broker = zookeeperClient.getBrokerInfo(config.getBrokerId());
        assertEquals(config.getBrokerId(), broker.id());
        assertEquals(config.getHostName(), broker.host());
        assertEquals(config.getPort(), broker.port());
    }

    @Test
    public void testGetPartitionReplicaLeaderInfo() {
        String topicName = "testTopic";
        var leaderAndReplicas = List.of(
                new LeaderAndReplicas(
                        new TopicAndPartition(topicName, 0),
                        new PartitionInfo(1, Arrays.asList(randomBroker(1),
                                randomBroker(2), randomBroker(3)
                ))));

        zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas);

        List<LeaderAndReplicas> leaderInfo =
                zookeeperClient.getPartitionReplicaLeaderInfo(topicName);
        assertEquals(1, leaderInfo.size());

        LeaderAndReplicas lar = leaderInfo.iterator().next();
        assertEquals(topicName, lar.topicPartition().topic());
        assertEquals(0, lar.topicPartition().partition());
        assertEquals(1, lar.partitionStateInfo().leaderBrokerId());
    }

    private static Broker randomBroker(int id) {
        return new Broker(id, "10.10.10.10", new Random().nextInt(10000));
    }
}