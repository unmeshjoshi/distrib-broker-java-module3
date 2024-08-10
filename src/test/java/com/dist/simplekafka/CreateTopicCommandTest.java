package com.dist.simplekafka;

import java.util.*;

import com.dist.common.Config;
import com.dist.common.ZookeeperTestHarness;
import org.I0Itec.zkclient.IZkChildListener;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class CreateTopicCommandTest extends ZookeeperTestHarness {
       @Test
    public void shouldCreatePersistentPathForTopicWithTopicPartitionAssignmentsInZookeeper() throws Exception {

        zookeeperClient.registerBroker(new Broker(0, "10.10.10.10", 8000));
        zookeeperClient.registerBroker(new Broker(1, "10.10.10.11", 8001));
        zookeeperClient.registerBroker(new Broker(2, "10.10.10.12", 8002));

        CreateTopicCommand createCommandTest =
                new CreateTopicCommand(zookeeperClient,
                        new ReplicaAssigner(new Random(100)));
        createCommandTest.createTopic("topic1", 2, 3);

        Map<String, List<PartitionReplicas>> topics = zookeeperClient.getAllTopics();
        assertEquals(1, topics.size());

        List<PartitionReplicas> partitionAssignments = zookeeperClient.getPartitionAssignmentsFor("topic1");
        assertEquals(2, partitionAssignments.size());
        partitionAssignments.forEach(p -> assertEquals(3, p.getBrokerIds().size()));
    }
}