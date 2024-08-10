package com.dist.simplekafka;

import java.util.List;

public class ReplicaAssignmentStrategy {
    public List<Integer> assignReplica(int partitionId, int replicationFactor, List<Integer> brokerIds) {
        return brokerIds.subList(0, replicationFactor);
    }
}