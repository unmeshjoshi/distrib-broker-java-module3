package com.dist.simplekafka;

import com.dist.common.JsonSerDes;
import com.dist.net.InetAddressAndPort;
import com.dist.net.RequestKeys;
import com.dist.net.RequestOrResponse;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ZkController {
    private final ZookeeperClient zookeeperClient;
    private final int brokerId;
    private final BrokerNetworkHandler socketServer;
    private final AtomicInteger correlationId = new AtomicInteger(0);
    private Set<Broker> liveBrokers = new HashSet<>();
    private int currentLeader = -1;

    public ZkController(ZookeeperClient zookeeperClient, int brokerId, BrokerNetworkHandler socketServer) {
        this.zookeeperClient = zookeeperClient;
        this.brokerId = brokerId;
        this.socketServer = socketServer;
    }

    public void startup() {
        zookeeperClient.subscribeControllerChangeListener(this);
        elect();
    }

    public void electNewLeaderForPartition() {
        List<String> topics = zookeeperClient.getTopics();
        List<LeaderAndReplicas> newLeaderAndReplicaList = new ArrayList<>();
        List<PartitionReplicas> partitionReplicas = new ArrayList<>();

        for (String topicName : topics) {
            List<LeaderAndReplicas> leaderAndReplicaList =
                    zookeeperClient.getPartitionReplicaLeaderInfo(topicName);
            boolean leaderChanged = false;

            for (int i = 0; i < leaderAndReplicaList.size(); i++) {
                LeaderAndReplicas leaderAndReplica = leaderAndReplicaList.get(i);
                PartitionInfo partitionInfo = leaderAndReplica.partitionStateInfo();
                Broker partitionCurrentLeader = partitionInfo.getLeaderBroker();

                partitionReplicas.addAll(zookeeperClient.getPartitionAssignmentsFor(topicName));

                if (!liveBrokers.contains(partitionCurrentLeader)) {
                    for (Broker replica : partitionInfo.allBrokers()) {
                        if (!partitionCurrentLeader.equals(replica)) {
                            partitionCurrentLeader = replica;
                            PartitionInfo newPartitionInfo =
                                    new PartitionInfo(partitionCurrentLeader.id(),
                                            partitionInfo.allBrokers());
                            LeaderAndReplicas leaderReplica = new LeaderAndReplicas(leaderAndReplica.topicPartition(), newPartitionInfo);
                            leaderAndReplicaList.set(i, leaderReplica);
                            leaderChanged = true;
                            break;
                        }
                    }
                }
            }

            if (leaderChanged) {
                zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicaList);
                newLeaderAndReplicaList.addAll(leaderAndReplicaList);
            }
        }

        if (!newLeaderAndReplicaList.isEmpty()) {
            sendUpdateMetadataRequestToAllLiveBrokers(newLeaderAndReplicaList);
            sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(newLeaderAndReplicaList, partitionReplicas);
        }
    }

    public void shutdown() {
        // Implementation not provided in the original code
    }

    public void elect() {
        try {
            zookeeperClient.tryCreatingControllerPath(brokerId);
            this.currentLeader = brokerId;
            onBecomingLeader();
        } catch (ControllerExistsException e) {
            this.currentLeader = e.getControllerId();
        }
    }

    private void onBecomingLeader() {
        liveBrokers.addAll(zookeeperClient.getAllBrokers());
        zookeeperClient.subscribeTopicChangeListener(new TopicChangeHandler(zookeeperClient, this));
        zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListener(this));
    }

    /**
     * Handles the creation of a new topic by:
     * 1. Selecting leaders and followers for each partition
     * 2. Persisting the leader/follower information to ZooKeeper
     * 3. Notifying relevant brokers about their leader/follower roles
     * 4. Updating all brokers with the new metadata
     *
     * @param topicName         Name of the new topic being created
     * @param partitionReplicas List of partition assignments with their replica brokers
     */

    public void handleNewTopic(String topicName, List<PartitionReplicas> partitionReplicas) {
        List<LeaderAndReplicas> leaderAndReplicas = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas);
        zookeeperClient.setPartitionLeaderForTopic(topicName,
                leaderAndReplicas);
        //Assignment send leader follower information to individual brokers.

        sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas);

        //Assignment send all the metadata information to individual brokers.
        sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas);
    }

    /**
     * Assigns leader and follower brokers for each partition of a topic.
     * <p>
     * The partitionReplicas input contains broker assignments that were already
     * carefully determined during partition assignment, considering:
     * - Even distribution of partitions across brokers
     * - Rack awareness (Not implemented)
     * - Broker load balancing etc.. (Not implemented)
     * <p>
     * Therefore, selecting the first broker from each partition's replica list as leader is valid because:
     * 1. The replica list order was already optimized during partition assignment
     * 2. This ensures deterministic leader assignment (same input always yields same leader)
     * 3. Leaders will be naturally distributed across brokers due to the balanced partition assignment
     * <p>
     * Example:
     * Input partitionReplicas: [
     * Partition 0 => brokers [1,2,3],
     * Partition 1 => brokers [2,3,1],
     * Partition 2 => brokers [3,1,2]
     * ]
     * <p>
     * Results in leaders:
     * - Partition 0 => Leader: 1, Followers: 2,3
     * - Partition 1 => Leader: 2, Followers: 3,1
     * - Partition 2 => Leader: 3, Followers: 1,2
     *
     * @param topicName         Name of the topic
     * @param partitionReplicas List of partition assignments with their replica brokers
     * @return List of leader and replica assignments for each partition
     */
    private List<LeaderAndReplicas> selectLeaderAndFollowerBrokersForPartitions(String topicName, List<PartitionReplicas> partitionReplicas) {
        //Assignment assign leader and follower to partitions.
        return partitionReplicas.stream().map(p -> {
            int leaderBrokerId = p.brokerIds().get(0); //mark first one as
            // broker.
            List<Broker> replicaBrokers = p.brokerIds().stream().map(this::getBroker).collect(Collectors.toList());
            return new LeaderAndReplicas(new TopicAndPartition(topicName,
                    p.getPartitionId()), new PartitionInfo(leaderBrokerId,
                    replicaBrokers));
        }).collect(Collectors.toList());
    }

    private Broker getBroker(int brokerId) {
        return liveBrokers.stream().filter(b -> b.id() == brokerId).findFirst().orElseThrow();
    }


    private void sendUpdateMetadataRequestToAllLiveBrokers(List<LeaderAndReplicas> leaderAndReplicas) {
        for (Broker broker : liveBrokers) {
            UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest(new ArrayList<>(liveBrokers), leaderAndReplicas);
            RequestOrResponse request = new RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet());
            socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host(), broker.port()));
        }
    }

    /**
     * Sends leader and replica assignments to all involved brokers.
     * Each broker receives information about the partitions where it serves
     * either as a leader or as a follower.
     *
     * @param leaderAndReplicas List of leader and replica assignments
     * @param partitionReplicas List of partition assignments with their replica brokers
     */
    public void sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(List<LeaderAndReplicas> leaderAndReplicas, List<PartitionReplicas> partitionReplicas) {
        Map<Broker, List<LeaderAndReplicas>> brokerToLeaderIsrRequest = new HashMap<>();

        for (LeaderAndReplicas lr : leaderAndReplicas) {
            for (Broker broker : lr.partitionStateInfo().allBrokers()) {
                brokerToLeaderIsrRequest.computeIfAbsent(broker, k -> new ArrayList<>()).add(lr);
            }
        }

        for (Map.Entry<Broker, List<LeaderAndReplicas>> entry : brokerToLeaderIsrRequest.entrySet()) {
            Broker broker = entry.getKey();
            List<LeaderAndReplicas> leaderAndReplicasList = entry.getValue();
            LeaderAndReplicaRequest leaderAndReplicaRequest = new LeaderAndReplicaRequest(leaderAndReplicasList);
            RequestOrResponse request = new RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement());
            socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host(), broker.port()));
        }
    }

    public void addBroker(Broker broker) {
        liveBrokers.add(broker);
    }

    public void onBrokerStartup(List<Integer> brokerIds) {
        // We do not do anything, assuming all topics are created after all the brokers are up and running
    }

    public void setCurrent(int existingControllerId) {
        this.currentLeader = existingControllerId;
    }

    public int getBrokerId() {
        return this.brokerId;
    }

    public Set<Broker> getLiveBrokers() {
        return liveBrokers;
    }

    public int getCurrentLeaderId() {
        return currentLeader;
    }

    void processRemovedBrokers(Set<Integer> removedBrokerIds) {
        Iterator<Broker> iterator = liveBrokers.iterator();
        while (iterator.hasNext()) {
            Broker broker = iterator.next();
            if (removedBrokerIds.contains(broker.id())) {
                iterator.remove();
                System.out.println("Removing broker = " + broker);

                onBrokerRemoved(broker);  // Additional action when a broker is removed
            }
        }
    }


    private void onBrokerRemoved(Broker broker) {
        // Perform any additional actions needed when a broker is removed
        // For example: logging, notifying other components, cleanup, etc.
    }

    void processNewlyAddedBrokers(Set<Integer> newBrokerIds) {
        for (int newBrokerId : newBrokerIds) {
            Broker broker = zookeeperClient.getBrokerInfo(newBrokerId);
            System.out.println("Adding broker = " + broker);
            addBroker(broker);
        }
    }

    Set<Integer> getCurrentLiveBrokerIds() {
        return getLiveBrokers().stream().map(Broker::id).collect(Collectors.toSet());
    }

}
