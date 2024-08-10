package com.dist.simplekafka;

import java.util.*;
import java.util.stream.Collectors;

public class ReplicaAssigner {
    final Random random;

    public ReplicaAssigner(Random random) {
        this.random = random;
    }

    public ReplicaAssigner() {
        this(new Random());
    }

    public Set<PartitionReplicas> assignReplicasToBrokers(List<Integer> brokerList, int nPartitions, int replicationFactor) {
        Map<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = random.nextInt(brokerList.size());
        int currentPartitionId = 0;
        int nextReplicaShift = random.nextInt(brokerList.size());

        for (int partitionId = 0; partitionId < nPartitions; partitionId++) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0)) {
                nextReplicaShift++;
            }
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List<Integer> replicaList = new ArrayList<>();
            replicaList.add(brokerList.get(firstReplicaIndex));

            for (int j = 0; j < replicationFactor - 1; j++) {
                int index = getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size());
                replicaList.add(brokerList.get(index));
            }
            ret.put(currentPartitionId, replicaList);
            currentPartitionId++;
        }

        return ret.keySet().stream()
                .map(id -> new PartitionReplicas(id, ret.get(id)))
                .collect(Collectors.toSet());
    }

    private int getWrappedIndex(int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nBrokers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (firstReplicaIndex + shift) % nBrokers;
    }
}
