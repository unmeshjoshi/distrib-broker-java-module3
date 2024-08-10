package com.dist.simplekafka;

import org.I0Itec.zkclient.IZkChildListener;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopicChangeHandler implements IZkChildListener {
    private final ZookeeperClient zookeeperClient;
    private final ZkController controller;
    private Set<String> allTopics = new HashSet<>();

    public TopicChangeHandler(ZookeeperClient zookeeperClient,
                              ZkController controller) {
        this.zookeeperClient = zookeeperClient;
        this.controller = controller;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
        Set<String> newTopics = getNewlyAddedTopics(currentChildren);

//        getDeletedTopics(currentChildren); //not handling deleted topics as
//        of now.

        allTopics = new HashSet<>(currentChildren);

        newTopics.forEach(topicName -> {
            List<PartitionReplicas> replicas = zookeeperClient.getPartitionAssignmentsFor(topicName);
            controller.handleNewTopic(topicName, replicas);
        });
    }

    private void getDeletedTopics(List<String> currentChildren) {
        Set<String> deletedTopics = new HashSet<>(allTopics);
        deletedTopics.removeAll(currentChildren);
    }

    private Set<String> getNewlyAddedTopics(List<String> currentChildren) {
        Set<String> newTopics = new HashSet<>(currentChildren);
        newTopics.removeAll(allTopics);
        return newTopics;
    }
}
