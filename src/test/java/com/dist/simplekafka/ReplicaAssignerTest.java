package com.dist.simplekafka;


import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;


public class ReplicaAssignerTest {
    private ReplicaAssigner assigner;
    private List<Integer> brokerList;

    @Before
    public void setUp()  {
        assigner = new ReplicaAssigner(new Random(42));
        brokerList = List.of(1, 2, 3);

    }

    @Test
    public void testCorrectNumberOfPartitionsAndReplicas() {
        int nPartitions = 3;
        int replicationFactor = 2;

        Set<PartitionReplicas> result = assigner.assignReplicasToBrokers(brokerList, nPartitions, replicationFactor);

        assertEquals(nPartitions, result.size());
        for (PartitionReplicas pr : result) {
            assertEquals(replicationFactor, pr.getBrokerIds().size());
        }
    }

    @Test
    public void testReplicasAreOnDifferentBrokers() {
        int nPartitions = 3;
        int replicationFactor = 2;

        Set<PartitionReplicas> result = assigner.assignReplicasToBrokers(brokerList, nPartitions, replicationFactor);

        for (PartitionReplicas pr : result) {
            assertEquals(pr.brokerIds().size(), new HashSet<>(pr.brokerIds()).size());
        }
    }

    @Test
    public void testEvenDistributionOfReplicas() {
        int nPartitions = 1024;
        int replicationFactor = 3;

        Set<PartitionReplicas> result = assigner.assignReplicasToBrokers(brokerList, nPartitions, replicationFactor);

        Map<Integer, Integer> brokerUsageCount = new HashMap<>();
        for (PartitionReplicas pr : result) {
            for (int broker : pr.brokerIds()) {
                brokerUsageCount.put(broker, brokerUsageCount.getOrDefault(broker, 0) + 1);
            }
        }

        int expectedUsage = (nPartitions * replicationFactor) / brokerList.size();
        int tolerance = 2; // Allow some variance

        for (int count : brokerUsageCount.values()) {
            assertTrue(Math.abs(count - expectedUsage) <= tolerance);
//            "Each broker should be used approximately the same number of times"
        }
    }
}