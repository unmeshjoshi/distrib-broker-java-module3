package com.dist.simplekafka;

import com.dist.common.Config;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicaManager {
    private final Config config;
    private final HashMap<TopicAndPartition, Partition> allPartitions = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public ReplicaManager(Config config) {
        this.config = config;
    }

    public void makeFollower(TopicAndPartition topicAndPartition, Broker leader) {
        lock.lock();
        try {
            Partition partition = getOrCreatePartition(topicAndPartition);
            partition.makeFollower(leader);
        } finally {
            lock.unlock();
        }
    }

    public void makeLeader(TopicAndPartition topicAndPartition) {
        lock.lock();
        try {
            Partition partition = getOrCreatePartition(topicAndPartition);
            partition.makeLeader();
        } finally {
            lock.unlock();
        }
    }

    public Partition getPartition(TopicAndPartition topicAndPartition) {
        lock.lock();
        try {
            return allPartitions.get(topicAndPartition);
        } finally {
            lock.unlock();
        }
    }

    public Partition getOrCreatePartition(TopicAndPartition topicAndPartition) {
        lock.lock();
        try {
            Partition partition = allPartitions.get(topicAndPartition);
            if (partition == null) {
                partition = new Partition(config, topicAndPartition);
                allPartitions.put(topicAndPartition, partition);
            }
            return partition;
        } catch(Exception e) {
            throw new RuntimeException(e);

        } finally {
            lock.unlock();
        }
    }

    public HashMap<TopicAndPartition, Partition> getAllPartitions() {
        return allPartitions;
    }
}
