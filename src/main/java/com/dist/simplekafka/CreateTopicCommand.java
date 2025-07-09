package com.dist.simplekafka;

import java.util.ArrayList;
import java.util.Set;

public class CreateTopicCommand {
    private ZookeeperClient zookeeperClient;
    private ReplicaAssigner replicaAssigner;

    public CreateTopicCommand(ZookeeperClient zookeeperClient,
                              ReplicaAssigner replicaAssigner) {
        this.zookeeperClient = zookeeperClient;
        this.replicaAssigner = replicaAssigner;
    }

    public void createTopic(String topicName, int noOfPartitions, int replicationFactor) {
        createTopicInZookeeper(topicName, noOfPartitions, replicationFactor);
    }

    private void createTopicInZookeeper(String topicName, int noOfPartitions, int replicationFactor) {
        Set<Integer> brokerIds = zookeeperClient.getAllBrokerIds();
        Set<PartitionReplicas> partitionReplicas =
                this.replicaAssigner.assignReplicasToBrokers(new ArrayList<>(brokerIds),
                        noOfPartitions,
                        replicationFactor);
        zookeeperClient.setPartitionReplicasForTopic(topicName,
                new ArrayList(partitionReplicas));
    }
}
